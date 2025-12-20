defmodule NamedSQLTest do
  use ExUnit.Case
  doctest NamedSQL

  alias NamedSQL.Repo
  require Repo

  describe "named_sql/2 (macro-based compile time checks)" do
    test "rewrites named params to positional and ordered parameters" do
      result =
        Repo.named_sql(
          "SELECT $b, $a, $b",
          a: 1,
          b: 2
        )

      [%{"a" => sql, "b" => params}] = result

      assert sql == "SELECT $1, $2, $1"
      assert params == [2, 1]
    end

    test "raises at compile time for missing param keys (literal opts)" do
      assert_compile_time_raise(ArgumentError, ~r/missing keys/i, fn ->
        """
        defmodule NamedSQL.CTMissing do
          alias NamedSQL.Repo
          require Repo

          def run do
            Repo.named_sql("SELECT $id", [])
          end
        end
        """
      end)

      assert_compile_time_raise(ArgumentError, ~r/missing keys/i, fn ->
        """
        defmodule NamedSQL.CTMissing2 do
          alias NamedSQL.Repo
          require Repo

          def run do
            Repo.named_sql("SELECT $id, $name", id: 1)
          end
        end
        """
      end)
    end

    test "raises at compile time for additional param keys (literal opts)" do
      assert_compile_time_raise(ArgumentError, ~r/additional keys/i, fn ->
        """
        defmodule NamedSQL.CTAdditional do
          alias NamedSQL.Repo
          require Repo

          def run do
            Repo.named_sql("SELECT $id", id: 1, extra: 2)
          end
        end
        """
      end)
    end

    test "raises at compile time when opts are not a literal keyword list" do
      assert_compile_time_raise(ArgumentError, ~r/literal.*keyword list/i, fn ->
        """
        defmodule NamedSQL.CTDynamicOpts do
          alias NamedSQL.Repo
          require Repo

          def run(id) do
            opts = [id: id]
            Repo.named_sql("SELECT $id", opts)
          end
        end
        """
      end)
    end

    test "raises at compile time if query uses reserved option names as placeholders" do
      assert_compile_time_raise(ArgumentError, ~r/reserved parameter names/i, fn ->
        """
        defmodule NamedSQL.CTReserved do
          alias NamedSQL.Repo
          require Repo

          def run do
            Repo.named_sql("SELECT $result_mapper", result_mapper: fn _ -> :ok end)
          end
        end
        """
      end)
    end
  end

  describe "named_sql_dynamic/2 (runtime checks)" do
    test "accepts dynamic opts and validates missing/additional keys at runtime" do
      assert_raise ArgumentError, ~r/missing keys/i, fn ->
        Repo.named_sql_dynamic("SELECT $id", [])
      end

      assert_raise ArgumentError, ~r/additional keys/i, fn ->
        Repo.named_sql_dynamic("SELECT $id", id: 1, extra: 2)
      end

      # success path
      opts = [id: 123]

      [%{"a" => sql, "b" => params}] =
        Repo.named_sql_dynamic("SELECT $id", opts)

      assert sql == "SELECT $1"
      assert params == [123]
    end

    test "respects result_mapper" do
      result =
        Repo.named_sql_dynamic(
          "SELECT $id",
          id: 9,
          result_mapper: fn [sql, params] -> %{sql: sql, params: params} end
        )

      assert [%{sql: "SELECT $1", params: [9]}] = result
    end
  end

  defp assert_compile_time_raise(exception, message_regex, fun) do
    code = fun.()

    assert_raise exception, message_regex, fn ->
      Code.compile_string(code)
    end
  end
end
