"""
Stored messages: ids, history on JOIN, paging, resuming after a disconnect,
and the hand-over from history to live messages (no gaps, no duplicates).
These run on the single-process backend; tests/test_cluster_redis.py repeats
the important ones across two servers sharing Redis.
"""
import asyncio

import pytest

from app.client.common import RoomCursor, render
from app.client.enhanced_client import EnhancedChatClient
from app.core.config import ClientConfig
from app.server.cluster import LocalCluster, parse_message_id
from tests.support import drain, running, texts, user


def ids_increase(frames):
    keys = [parse_message_id(f["id"]) for f in frames if f.get("type") == "MSG"]
    return all(a < b for a, b in zip(keys, keys[1:]))


async def test_messages_get_increasing_ids_and_history_replays_them():
    async with running() as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        sent = [await alice.say("r", f"m{i}") for i in range(5)]
        assert all(parse_message_id(m["id"]) for m in sent) and ids_increase(sent)

        bob = await user(srv.test_port, "bob")
        await bob.send({"type": "JOIN", "room": "r", "history": 3})
        frames = await bob.recv_until("HISTORY_END")
        assert frames[0]["type"] == "JOINED"
        history = [f for f in frames if f.get("history")]
        assert texts(history) == ["m2", "m3", "m4"]               # newest 3, oldest first
        assert [f["id"] for f in history] == [m["id"] for m in sent[2:]]
        assert frames[-1] == {"type": "HISTORY_END", "room": "r", "count": 3, "more": True}
        await alice.close()
        await bob.close()


async def test_join_without_history_is_unchanged():
    async with running() as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        await alice.say("r", "before")
        bob = await user(srv.test_port, "bob")
        assert (await bob.join("r"))["type"] == "JOINED"
        assert [f["type"] for f in await drain(bob)] == []        # no history frames by default
        await alice.close()
        await bob.close()


async def test_history_pages_backwards_to_the_start():
    async with running(history_page_max=4, enable_rate_limiting=False) as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        for i in range(10):
            await alice.say("r", f"m{i}")
        seen, before = [], None
        while True:
            request = {"type": "HISTORY", "room": "r", "limit": 50}
            if before:
                request["before"] = before
            await alice.send(request)
            frames = await alice.recv_until("HISTORY_END")
            page = [f for f in frames if f.get("type") == "MSG"]
            assert len(page) <= 4                                  # capped by history_page_max
            seen = texts(page) + seen
            if not frames[-1]["more"]:
                break
            before = page[0]["id"]
        assert seen == [f"m{i}" for i in range(10)]
        await alice.close()


async def test_join_since_resumes_exactly_after_the_last_message_seen():
    async with running() as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        first = await alice.say("r", "seen")
        for i in range(3):
            await alice.say("r", f"missed{i}")
        bob = await user(srv.test_port, "bob")
        await bob.send({"type": "JOIN", "room": "r", "since": first["id"]})
        frames = await bob.recv_until("HISTORY_END")
        assert texts(frames) == ["missed0", "missed1", "missed2"]
        assert frames[-1]["more"] is False and "gap" not in frames[-1]
        await alice.close()
        await bob.close()


async def test_resume_after_a_long_absence_joins_up_with_live_messages():
    async with running(history_resume_max=3, enable_rate_limiting=False) as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        first = await alice.say("r", "seen")
        for i in range(10):
            await alice.say("r", f"m{i}")
        bob = await user(srv.test_port, "bob")
        await bob.send({"type": "JOIN", "room": "r", "since": first["id"]})
        frames = await bob.recv_until("HISTORY_END")
        # The newest 3, not the oldest 3: nothing between them and the live
        # stream is skipped. The older ones are flagged and pageable.
        assert texts(frames) == ["m7", "m8", "m9"]
        assert frames[-1]["gap"] is True and frames[-1]["more"] is True
        await alice.say("r", "live")
        assert texts(await drain(bob)) == ["live"]
        await alice.close()
        await bob.close()


async def test_history_to_live_handover_has_no_gaps_or_duplicates():
    """Messages sent while a JOIN's history is being read must appear exactly
    once, in order, after the history."""
    async with running() as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        for i in range(3):
            await alice.say("r", f"old{i}")

        real_history = srv.cluster.history
        reading = asyncio.Event()

        async def slow_history(room, **kwargs):
            reading.set()
            await asyncio.sleep(0.3)        # these messages land in BOTH the
            return await real_history(room, **kwargs)  # stored log and the live feed

        srv.cluster.history = slow_history
        bob = await user(srv.test_port, "bob")
        await bob.send({"type": "JOIN", "room": "r", "history": 50})
        await reading.wait()
        for i in range(3):
            await alice.say("r", f"during{i}")
        srv.cluster.history = real_history
        await alice.say("r", "after")

        frames = await drain(bob, 0.5)
        types = [f["type"] for f in frames]
        assert types.index("HISTORY_END") > types.index("JOINED")
        everything = texts(frames)
        assert everything == ["old0", "old1", "old2", "during0", "during1", "during2", "after"]
        assert ids_increase(frames)
        await alice.close()
        await bob.close()


@pytest.mark.parametrize("frame, error", [
    ({"type": "JOIN", "room": "r", "since": "yesterday"}, "Invalid 'since'"),
    ({"type": "JOIN", "room": "r", "history": -1}, "Invalid 'history'"),
    ({"type": "JOIN", "room": "r", "history": True}, "Invalid 'history'"),
    ({"type": "HISTORY", "room": "elsewhere"}, "not in room"),
    ({"type": "HISTORY", "room": "r", "before": "x-y"}, "Invalid 'before'"),
    ({"type": "HISTORY", "room": "r", "limit": 0}, "Invalid 'limit'"),
])
async def test_history_requests_are_validated(frame, error):
    async with running() as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        await alice.send(frame)
        reply = await alice.recv()
        assert reply["type"] == "ERROR" and error in reply["message"]
        await alice.close()


async def test_presence_notices_are_not_stored():
    async with running() as srv:
        alice = await user(srv.test_port, "alice")
        await alice.join("r")
        bob = await user(srv.test_port, "bob")
        await bob.join("r")
        await alice.say("r", "hello")
        carol = await user(srv.test_port, "carol")
        await carol.send({"type": "JOIN", "room": "r", "history": 50})
        frames = await carol.recv_until("HISTORY_END")
        assert [f["type"] for f in frames if f.get("history")] == ["MSG"]
        for c in (alice, bob, carol):
            await c.close()


async def test_local_history_is_bounded_per_room_and_in_bytes():
    cluster = LocalCluster(history_per_room=5, max_bytes=2000)
    cluster.bind(lambda *a: asyncio.sleep(0), lambda room: 0, lambda: [], lambda name: None)
    for i in range(8):
        await cluster.publish("a", {"text": f"a{i}"}, persist=True)
    entries, more = await cluster.history("a", limit=50)
    assert [m["text"] for _, m in entries] == [f"a{i}" for i in range(3, 8)] and not more
    big = "x" * 300
    for i in range(6):                                  # ~364 bytes each: over the byte cap
        await cluster.publish("b", {"text": big}, persist=True)
    assert cluster._bytes <= 2000
    # The least recently active room loses its oldest messages first; the
    # busy room keeps all it is allowed.
    left = [m["text"] for _, m in (await cluster.history("a", limit=50))[0]]
    assert left and len(left) < 5 and left[-1] == "a7"
    assert len((await cluster.history("b", limit=50))[0]) == 5


def test_client_cursor_hides_repeats_and_tracks_paging_points():
    cursor = RoomCursor()
    live = {"type": "MSG", "room": "r", "username": "bob", "text": "hi", "id": "100-0"}
    assert render(live, "alice", cursor) == "[r] bob: hi"
    assert render(live, "alice", cursor) is None              # delivered twice: shown once
    assert render(dict(live, id="101-0", username="alice"), "alice", cursor) is None  # own echo
    assert cursor.newest["r"] == "101-0" and cursor.oldest["r"] == "100-0"

    # A resume with a gap: older messages must be paged from the new page.
    for mid in ("500-0", "501-0"):
        render({"type": "MSG", "room": "r", "username": "bob", "text": mid, "id": mid,
                "history": True, "timestamp": "2026-11-13T10:00:00"}, "alice", cursor)
    text = render({"type": "HISTORY_END", "room": "r", "count": 2, "more": True, "gap": True}, "alice", cursor)
    assert "while you were away" in text and cursor.oldest["r"] == "500-0"


def test_resilient_client_rejoins_from_the_last_message_seen():
    cli = EnhancedChatClient("127.0.0.1", 1, ClientConfig())
    cli._rooms = {"seen", "fresh"}
    cli.cursor.note("seen", "1700000000000-3")
    assert cli._join_frame("seen") == {"type": "JOIN", "room": "seen", "since": "1700000000000-3"}
    assert cli._join_frame("fresh") == {"type": "JOIN", "room": "fresh", "history": 50}
