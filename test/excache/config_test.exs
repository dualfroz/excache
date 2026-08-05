defmodule Excache.ConfigTest do
  use ExUnit.Case, async: false

  alias Excache.Config
  alias Excache.Eviction
  alias Excache.Eviction.Tracker

  setup do
    Config.reset()
    on_exit(&Config.reset/0)
    :ok
  end

  test "maxmemory accepts the Redis unit suffixes and refuses everything else" do
    sizes = [
      {"0", 0},
      {"100", 100},
      {"512b", 512},
      {"1k", 1000},
      {"1kb", 1024},
      {"1m", 1_000_000},
      {"8MB", 8 * 1024 * 1024},
      {"1g", 1_000_000_000},
      {"2gb", 2 * 1024 * 1024 * 1024}
    ]

    for {raw, bytes} <- sizes do
      assert Config.set("maxmemory", raw) == :ok
      assert Config.maxmemory() == bytes
    end

    for raw <- ["", "abc", "-1", "1 mb", "1tb", "kb", "1.5mb", "1kbb", "mb1"] do
      assert {:error, {:invalid, hint}} = Config.set("maxmemory", raw)
      assert hint =~ "memory value"
    end

    assert Config.maxmemory() == 2 * 1024 * 1024 * 1024
  end

  test "the default policy is noeviction and every Redis spelling round trips" do
    assert Config.policy() == :noeviction
    assert Config.get("maxmemory-policy") == {:ok, :noeviction}

    for policy <- Eviction.policies() do
      name = Eviction.policyName(policy)
      assert Config.set("maxmemory-policy", name) == :ok
      assert Config.policy() == policy
      assert Config.getGlob("maxmemory-policy") == [{"maxmemory-policy", name}]
    end

    assert {:error, {:invalid, hint}} = Config.set("maxmemory-policy", "lru")
    assert hint =~ "allkeys-lru"
    assert Config.policy() == :volatileRandom
  end

  test "only the lru and lfu policies switch access tracking on" do
    refute Config.tracksAccess?()

    for name <- ~w(allkeys-lru allkeys-lfu volatile-lru volatile-lfu) do
      assert Config.set("maxmemory-policy", name) == :ok
      assert Config.tracksAccess?()
    end

    for name <- ~w(noeviction allkeys-random volatile-random volatile-ttl) do
      assert Config.set("maxmemory-policy", name) == :ok
      refute Config.tracksAccess?()
    end
  end

  test "integer parameters are range checked and keep the last valid value" do
    assert Config.maxmemorySamples() == Eviction.defaultSampleSize()
    assert Config.set("maxmemory-samples", "1") == :ok
    assert Config.set("maxmemory-samples", "64") == :ok
    assert Config.maxmemorySamples() == 64

    for raw <- ["0", "65", "ten", "-3", ""] do
      assert {:error, {:invalid, hint}} = Config.set("maxmemory-samples", raw)
      assert hint == "argument must be an integer between 1 and 64"
    end

    assert Config.maxmemorySamples() == 64
  end

  test "the lfu knobs reach the running tracker and come back on reset" do
    assert Tracker.settings(Tracker) == %{logFactor: 10, decayTime: 1}

    assert Config.set("lfu-log-factor", "42") == :ok
    assert Config.set("lfu-decay-time", "7") == :ok
    assert Config.lfuLogFactor() == 42
    assert Config.lfuDecayTime() == 7
    assert Tracker.settings(Tracker) == %{logFactor: 42, decayTime: 7}

    Config.reset()
    assert Tracker.settings(Tracker) == %{logFactor: 10, decayTime: 1}
  end

  test "port and the aof path are readable but cannot be changed at runtime" do
    assert Config.get("port") == {:ok, Application.get_env(:excache, :port)}
    assert Config.set("port", "6380") == {:error, :immutable}

    assert Config.get("aof_path") == {:ok, Excache.Aof.path()}
    assert Config.set("aof_path", "/tmp/excache-should-not-move.aof") == {:error, :immutable}
    assert Config.get("aof_path") == {:ok, Excache.Aof.path()}
  end

  test "the snapshot path is settable and persistence follows it" do
    previous = Application.get_env(:excache, :snapshot_path)
    on_exit(fn -> Application.put_env(:excache, :snapshot_path, previous) end)

    target = Path.join(System.tmp_dir!(), "excache-config-test.snapshot")

    assert Config.set("snapshot_path", target) == :ok
    assert Excache.Persistence.snapshot_path() == target
    assert Config.get("snapshot_path") == {:ok, target}
    assert {:error, {:invalid, _hint}} = Config.set("snapshot_path", "")
    assert Excache.Persistence.snapshot_path() == target
  end

  test "unknown parameters are refused on read, write and glob" do
    assert Config.get("nosuch") == :error
    assert Config.set("nosuch", "1") == {:error, :unknown}
    assert Config.getGlob("nosuch*") == []
  end

  test "getGlob follows glob semantics and answers sorted, formatted pairs" do
    assert Config.getGlob("maxmemory") == [{"maxmemory", "0"}]

    assert Config.getGlob("maxmemory*") == [
             {"maxmemory", "0"},
             {"maxmemory-policy", "noeviction"},
             {"maxmemory-samples", "5"}
           ]

    assert Config.getGlob("lfu-*") == [{"lfu-decay-time", "1"}, {"lfu-log-factor", "10"}]
    assert Config.getGlob("?axmemory") == [{"maxmemory", "0"}]
    assert Config.getGlob("*policy") == [{"maxmemory-policy", "noeviction"}]
    assert Config.getGlob("MAXMEMORY") == [{"maxmemory", "0"}]
    assert Config.getGlob("maxmemor") == []

    assert Enum.map(Config.getGlob("*"), fn {name, _value} -> name end) == Config.names()
  end

  test "parameter names are case insensitive" do
    assert Config.set("MaxMemory", "2kb") == :ok
    assert Config.get("MAXMEMORY") == {:ok, 2048}
    assert Config.getGlob("maxmemory") == [{"maxmemory", "2048"}]
  end

  test "setMany applies every pair or leaves all of them alone" do
    assert Config.setMany([{"maxmemory", "4kb"}, {"maxmemory-policy", "allkeys-lfu"}]) == :ok
    assert Config.maxmemory() == 4096
    assert Config.policy() == :allkeysLfu

    assert {:error, "maxmemory-policy", {:invalid, _hint}} =
             Config.setMany([{"maxmemory", "8kb"}, {"maxmemory-policy", "nope"}])

    assert Config.maxmemory() == 4096
    assert Config.policy() == :allkeysLfu
  end
end
