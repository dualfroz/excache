defmodule Excache.Persistence do
  @moduledoc false

  def snapshot_path do
    Application.get_env(:excache, :snapshot_path) ||
      Path.join(to_string(:code.priv_dir(:excache)), "excache.snapshot")
  end

  def save do
    path = snapshot_path()
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, :erlang.term_to_binary(Excache.Store.dump()))
    :ok
  end

  def load_entries(path) do
    case File.read(path) do
      {:ok, binary} -> :erlang.binary_to_term(binary)
      {:error, _reason} -> []
    end
  end
end
