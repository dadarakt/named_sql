defmodule NamedSQL.Repo do
  @moduledoc """
  This is where you ususally define your Ecto Repo for your application
  """

  use NamedSQL, repo: NamedSQL.FixtureRepo
end
