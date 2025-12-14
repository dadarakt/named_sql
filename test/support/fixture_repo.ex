defmodule NamedSQL.FixtureRepo do
  @doc """
  A simple wrapper for the only function that is used from `Ecto.Repo`.
  Echoes back the generated query and params for testing inspection.
  """
  def query(sql, params) do
    {:ok, %{columns: ["a", "b"], rows: [[sql, params]]}}
  end
end
