defmodule Excache.Eviction.TrackerTest do
  use ExUnit.Case, async: true

  alias Excache.Eviction.Tracker

  @minute 60_000

  test "a fresh key starts at the Redis init counter with zero idle time" do
    tracker = startTracker(logFactor: 0)

    assert Tracker.touch(tracker, "alpha", 1_000) == Tracker.lfuInitCounter()
    assert Tracker.lookup(tracker, "alpha", 1_000) == %{lastAccess: 1_000, idleMs: 0, counter: 5}
    assert Tracker.lookup(tracker, "ghost", 1_000) == nil
    assert Tracker.counter(tracker, "ghost", 1_000) == 0
    assert Tracker.idleMs(tracker, "ghost", 1_000) == nil
  end

  test "a linear counter separates a key read a hundred times from one read twice" do
    tracker = startTracker(logFactor: 0)
    now = 1_000_000

    touchTimes(tracker, "hot", 100, now)
    touchTimes(tracker, "cold", 2, now)

    assert Tracker.counter(tracker, "hot", now) == 104
    assert Tracker.counter(tracker, "cold", now) == 6
  end

  test "the log factor compresses a hot counter far below its hit count" do
    tracker = startTracker(logFactor: 10)
    now = 1_000_000

    touchTimes(tracker, "hot", 100, now)
    touchTimes(tracker, "cold", 2, now)

    hot = Tracker.counter(tracker, "hot", now)
    cold = Tracker.counter(tracker, "cold", now)

    assert cold == 6
    assert hot > cold
    assert hot < 30
  end

  test "decay strips the advantage of a hot key that goes idle" do
    tracker = startTracker(logFactor: 0, decayTime: 1)
    now = 1_000_000

    touchTimes(tracker, "hot", 100, now)
    touchTimes(tracker, "cold", 2, now)

    partial = now + 5 * @minute
    assert Tracker.counter(tracker, "hot", partial) == 99
    assert Tracker.counter(tracker, "cold", partial) == 1

    faded = now + 200 * @minute
    assert Tracker.counter(tracker, "hot", faded) == 0
    assert Tracker.counter(tracker, "cold", faded) == 0

    Tracker.touch(tracker, "cold", faded)
    assert Tracker.counter(tracker, "cold", faded) == 1
    assert Tracker.counter(tracker, "cold", faded) > Tracker.counter(tracker, "hot", faded)
  end

  test "a decay time of zero freezes the counters" do
    tracker = startTracker(logFactor: 0, decayTime: 0)
    now = 1_000_000

    touchTimes(tracker, "hot", 100, now)

    assert Tracker.counter(tracker, "hot", now + 5000 * @minute) == 104
    assert Tracker.settings(tracker) == %{logFactor: 0, decayTime: 0}
  end

  test "the counter saturates at the eight bit ceiling" do
    tracker = startTracker(logFactor: 0, decayTime: 0)
    now = 1_000_000

    touchTimes(tracker, "hammered", 400, now)

    assert Tracker.counter(tracker, "hammered", now) == Tracker.lfuMaxCounter()
    assert Tracker.touch(tracker, "hammered", now) == 255
  end

  test "idle time is measured against the last access" do
    tracker = startTracker(logFactor: 0)
    now = 1_000_000

    Tracker.touch(tracker, "stale", now)
    Tracker.touch(tracker, "fresh", now + 30_000)

    assert Tracker.idleMs(tracker, "stale", now + 30_000) == 30_000
    assert Tracker.idleMs(tracker, "fresh", now + 30_000) == 0
    assert Tracker.lookup(tracker, "stale", now - 5_000).idleMs == 0
  end

  test "forget, retain and clear prune the metadata" do
    tracker = startTracker(logFactor: 0)
    now = 1_000_000

    Tracker.touchMany(tracker, ["a", "b", "c", "d"], now)
    assert Tracker.size(tracker) == 4

    Tracker.forget(tracker, "a")
    assert Tracker.size(tracker) == 3
    assert Tracker.lookup(tracker, "a", now) == nil

    Tracker.retain(tracker, ["b", "c"])
    assert Tracker.size(tracker) == 2
    assert Tracker.lookup(tracker, "d", now) == nil

    Tracker.clear(tracker)
    assert Tracker.size(tracker) == 0
    assert Tracker.snapshot(tracker, now) == %{}
  end

  test "metadata answers only for the requested keys and decays to the given time" do
    tracker = startTracker(logFactor: 0, decayTime: 1)
    now = 1_000_000

    touchTimes(tracker, "hot", 10, now)
    Tracker.touch(tracker, "cold", now)
    Tracker.touch(tracker, "other", now)

    metadata = Tracker.metadata(tracker, ["hot", "cold", "absent"], now + 3 * @minute)

    assert Map.keys(metadata) |> Enum.sort() == ["cold", "hot"]
    assert metadata["hot"].counter == 11
    assert metadata["cold"].counter == 2
    assert metadata["hot"].idleMs == 3 * @minute

    assert Tracker.snapshot(tracker, now) |> Map.keys() |> Enum.sort() == ["cold", "hot", "other"]
  end

  test "touchMany registers each key once and returns the new counters" do
    tracker = startTracker(logFactor: 0, decayTime: 0)
    now = 1_000_000

    assert Tracker.touchMany(tracker, ["a", "b"], now) == %{"a" => 5, "b" => 5}
    assert Tracker.touchMany(tracker, ["a", "a"], now) == %{"a" => 7}
    assert Tracker.counter(tracker, "a", now) == 7
  end

  test "the same seed replays the same logarithmic counter" do
    now = 1_000_000

    counters =
      for _run <- 1..2 do
        tracker = startTracker(logFactor: 10, seed: {3, 5, 7})
        touchTimes(tracker, "hot", 250, now)
        Tracker.counter(tracker, "hot", now)
      end

    assert [first, second] = counters
    assert first == second

    other = startTracker(logFactor: 10, seed: {11, 13, 17})
    touchTimes(other, "hot", 250, now)
    assert Tracker.counter(other, "hot", now) > 5
  end

  defp startTracker(opts) do
    id = {Tracker, System.unique_integer([:positive])}
    start_supervised!(Supervisor.child_spec({Tracker, opts}, id: id))
  end

  defp touchTimes(tracker, key, count, now) do
    Enum.each(1..count, fn _index -> Tracker.touch(tracker, key, now) end)
  end
end
