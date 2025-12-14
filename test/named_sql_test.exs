defmodule NamedSQLTest do
  use ExUnit.Case
  doctest NamedSQL

  test "greets the world" do
    assert NamedSQL.hello() == :world
  end
end
