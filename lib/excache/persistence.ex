defmodule Excache.Persistence do
  @moduledoc false

  def snapshot_path do
    Application.get_env(:excache, :snapshot_path) ||
      Path.join(to_string(:code.priv_dir(:excache)), "excache.snapshot")
  end

  def save do
    Excache.WriteGate.exclusive(fn ->
      path = snapshot_path()
      pendingPath = path <> ".tmp"
      File.mkdir_p!(Path.dirname(path))
      File.write!(pendingPath, :erlang.term_to_binary(Excache.Store.dump()), [:sync])
      File.rename!(pendingPath, path)
      Excache.Aof.truncate()
    end)
  end

  def load_entries(path) do
    case File.read(path) do
      {:ok, binary} -> :erlang.binary_to_term(binary)
      {:error, _reason} -> []
    end
  end
end
