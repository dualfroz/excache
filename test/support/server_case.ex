defmodule Excache.Test.ServerCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      alias Excache.Test.RespClient, as: Client
    end
  end

  setup do
    snapshot = Path.join(File.cwd!(), "tmp/snapshot_#{System.unique_integer([:positive])}.bin")
    aof = Path.join(File.cwd!(), "tmp/aof_#{System.unique_integer([:positive])}.bin")

    Application.put_env(:excache, :snapshot_path, snapshot)
    Application.put_env(:excache, :aof_path, aof)
    Excache.Store.flushdb()
    Excache.Aof.restart()

    {:ok, acceptor} = Excache.Server.Acceptor.start_link(port: 0)
    port = Excache.Server.Acceptor.port(acceptor)
    conn = Excache.Test.RespClient.connect(port)

    on_exit(fn ->
      Excache.Test.RespClient.close(conn)
      if Process.alive?(acceptor), do: GenServer.stop(acceptor)
      File.rm(snapshot)
      File.rm(aof)
      Excache.Store.flushdb()
      Application.delete_env(:excache, :snapshot_path)
      Application.delete_env(:excache, :aof_path)
    end)

    %{conn: conn, port: port}
  end
end
