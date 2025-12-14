defmodule NamedSQL do
  @moduledoc """
  NamedSQL provides a small, SQL-first abstraction for executing raw SQL queries
  with **named parameters** and **compile-time validation**.

  It is designed as a lightweight alternative to positional parameters (`$1`, `$2`, …)
  when using `Ecto.Repo.query/3`, while avoiding the complexity of query DSLs or ORMs.

  ## Key ideas

  - SQL is treated as a first-class language
  - Parameters are named (`$user_id`) instead of positional
  - Compile-time validation is applied whenever possible
  - No runtime atom creation
  - Minimal macro surface area

  ## Two execution paths

  NamedSQL provides **two explicit APIs**, depending on whether parameters are known
  at compile time or only at runtime:

  ### Compile-time validated (macro)

  `named_sql/2` is a macro that requires a **literal keyword list** for parameters.
  This enables compile-time validation of:

  - missing parameters
  - additional parameters
  - duplicate parameters
  - use of reserved option names in the SQL query

  If validation fails, compilation fails.

  ### Runtime validated (function)

  `named_sql_dynamic/2` is a regular function that accepts **dynamic keyword lists**.
  The same validations are applied, but at runtime instead of compile time.

  This explicit split avoids implicit contracts and makes dynamic behavior intentional.

  ## Typical usage

  NamedSQL is usually injected into an `Ecto.Repo`:

      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app
        use NamedSQL, repo: __MODULE__
      end

  And then used at the callsite:

      require MyApp.Repo

      MyApp.Repo.named_sql(
        "SELECT * FROM users WHERE id = $id",
        id: 1
      )
  """

  defstruct [:normalized_sql, :expected_keys, :ordered_keys]

  @type compiled :: %__MODULE__{
    normalized_sql: binary(),
    expected_keys: [String.t()],
    ordered_keys: [String.t()]
  }

  @reserved_keys [:result_mapper, :timeout, :log]
  @reserved_key_strings Enum.map(@reserved_keys, &Atom.to_string/1)

  defmacro __using__(opts) do
    repo = Keyword.get(opts, :repo) ||
      raise ArgumentError, "`:repo` is a required option: `use NamedSQL, repo: <Your Repo>`"

    quote do
      @named_sql_repo unquote(repo)

      @doc """
      Executes a SQL query with **named parameters**, performing **compile-time validation**.

      This macro requires the parameter list to be a **literal keyword list**.
      When this condition is met, the following validations are performed at compile time:

      - all parameters referenced in the SQL query are provided
      - no additional parameters are present
      - no duplicate parameter keys exist
      - reserved option names are not used as SQL placeholders

      If validation fails, compilation fails with a descriptive error.

      Because this is a macro, the calling module must `require` the repo.

      ## Example

          require Repo

          Repo.named_sql(
            "SELECT * FROM users WHERE id = $id",
            id: 1
          )

      ## Restrictions

      Passing a variable or dynamically constructed keyword list is not allowed.
      For dynamic parameters, use `named_sql_dynamic/2` instead.
      """
      defmacro named_sql(query_ast, opts_ast) do
        NamedSQL.__named_sql_macro__(@named_sql_repo, query_ast, opts_ast, __CALLER__)
      end

      # Explicit runtime-checked path (function): accepts dynamic keyword list
      def named_sql_dynamic(query, opts) when is_binary(query) do
        NamedSQL.named_sql_dynamic(@named_sql_repo, query, opts)
      end
    end
  end

  @doc false
  def __named_sql_macro__(repo, query_ast, opts_ast, caller) do
    query = Macro.expand(query_ast, caller)

    unless is_binary(query) do
      raise ArgumentError,
        "named_sql/2 expects a literal binary query at compile time, got: #{Macro.to_string(query_ast)}"
    end

    compiled = compile_sql!(query)

    provided_atoms =
      case literal_keyword_keys(opts_ast, caller) do
        {:ok, keys} ->
          keys
        :dynamic ->
          raise ArgumentError,
            "named_sql/2 requires a *literal* keyword list to enable compile-time validation. " <>
            "For dynamic params use named_sql_dynamic/2."
      end

    provided_strings = param_key_strings_from_atoms!(provided_atoms)
    validate_param_key_strings!(compiled.expected_keys, provided_strings)

    quote do
      opts = unquote(opts_ast)

      {params_by_string, result_mapper} = NamedSQL.params_by_string!(opts)
      params = NamedSQL.ordered_params!(params_by_string, unquote(Macro.escape(compiled.ordered_keys)))

      NamedSQL.execute(
        unquote(repo),
        unquote(Macro.escape(compiled)),
        params,
        result_mapper
      )
    end
  end

  @doc """
  Executes a SQL query with named parameters, performing validation **at runtime**.

  This function accepts dynamically constructed keyword lists and applies the same
  parameter validations as `named_sql/2`, but at runtime instead of compile time:

  - missing parameters
  - additional parameters
  - duplicate parameter keys
  - use of reserved option names in the SQL query

  This function exists as an explicit escape hatch for dynamic scenarios where
  compile-time validation is not possible.

  ## When to use this function

  Use `named_sql_dynamic/2` only when parameters cannot be provided as a literal
  keyword list. For all other cases, prefer `named_sql/2` to benefit from
  compile-time validation.
  """
  @spec named_sql_dynamic(module(), binary(), keyword()) :: list(any())
  def named_sql_dynamic(repo, query, opts) when is_binary(query) do
    compiled = compile_sql!(query)
    {params_by_string, result_mapper} = params_by_string!(opts)

    provided_strings = Map.keys(params_by_string)
    validate_param_key_strings!(compiled.expected_keys, provided_strings)

    params = ordered_params!(params_by_string, compiled.ordered_keys)
    execute(repo, compiled, params, result_mapper)
  end

  # Compile SQL placeholders ($name) to positional placeholders ($1) and return
  # expected/ordered keys as strings.
  defp compile_sql!(sql) when is_binary(sql) do
    param_regex = ~r/\$(?<name>[A-Za-z_][A-Za-z0-9_-]*)/

    extracted =
      Regex.scan(param_regex, sql, capture: :all_but_first)
      |> List.flatten()

    bad_reserved = Enum.filter(extracted, &(&1 in @reserved_key_strings))

    if bad_reserved != [] do
      raise ArgumentError,
        "named_sql: query uses reserved parameter names: #{inspect(bad_reserved)}. " <>
        "Reserved: #{inspect(@reserved_keys)}"
    end

    {mapping, _} =
      Enum.reduce(extracted, {%{}, 1}, fn(name, {acc, idx}) ->
        if Map.has_key?(acc, name), do: {acc, idx}, else: {Map.put(acc, name, idx), idx + 1}
      end)

    normalized =
      Enum.reduce(mapping, sql, fn({name, idx}, acc) ->
        String.replace(acc, "$#{name}", "$#{idx}")
      end)

    expected = Map.keys(mapping)

    ordered =
      mapping
      |> Enum.sort_by(fn({_name, idx}) -> idx end)
      |> Enum.map(fn({name, _idx}) -> name end)

    %__MODULE__{normalized_sql: normalized, expected_keys: expected, ordered_keys: ordered}
  end

  defp validate_param_key_strings!(expected_keys, provided_keys) do
    dupes =
      provided_keys
      |> Enum.frequencies()
      |> Enum.filter(fn({_k, n}) -> n > 1 end)
      |> Enum.map(&elem(&1, 0))

    if dupes != [] do
      raise ArgumentError, "named_sql: duplicate parameter keys: #{inspect(dupes)}"
    end

    missing = expected_keys -- provided_keys

    if missing != [] do
      raise ArgumentError,
        "named_sql: missing keys in params: #{inspect(missing)}. Expected: #{inspect(expected_keys)}"
    end

    additional = provided_keys -- expected_keys

    if additional != [] do
      raise ArgumentError,
        "named_sql: additional keys in params: #{inspect(additional)}. Expected: #{inspect(expected_keys)}"
    end

    :ok
  end

  defp param_key_strings_from_atoms!(keys_atoms) when is_list(keys_atoms) do
    keys_atoms
    |> Enum.reject(&(&1 in @reserved_keys))
    |> Enum.map(&Atom.to_string/1)
  end

  @doc false
  def params_by_string!(opts) when is_list(opts) do
    assert_keyword_opts!(opts)

    result_mapper = Keyword.get(opts, :result_mapper)
    params_kw = Keyword.drop(opts, @reserved_keys)

    {Map.new(params_kw, fn({k, v}) -> {Atom.to_string(k), v} end), result_mapper}
  end

  def params_by_string!(other) do
    raise ArgumentError, "named_sql expects a keyword list, got: #{inspect(other)}"
  end

  @doc false
  def ordered_params!(params_by_string, ordered_keys) when is_map(params_by_string) and is_list(ordered_keys) do
    Enum.map(ordered_keys, fn(k) -> Map.fetch!(params_by_string, k) end)
  end

  @doc false
  def execute(repo, %__MODULE__{normalized_sql: sql}, params, result_mapper) do
    with {:ok, %{rows: rows, columns: columns}} <- repo.query(sql, params) do
      map_fun =
        case result_mapper do
          nil ->
            fn(row) ->
              columns
              |> Enum.zip(row)
              |> Enum.into(%{})
            end

          mapper ->
            mapper
        end

      Enum.map(rows, map_fun)
    end
  end

  defp literal_keyword_keys(opts_ast, caller) do
    expanded = Macro.expand(opts_ast, caller)

    if is_list(expanded) and Keyword.keyword?(expanded) do
      {:ok, Enum.map(expanded, fn({k, _v}) -> k end)}
    else
      :dynamic
    end
  end

  @doc false
  def assert_keyword_opts!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "named_sql expects a keyword list, got: #{inspect(opts)}"
    end

    dupes =
      opts
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(fn({_k, n}) -> n > 1 end)
      |> Enum.map(&elem(&1, 0))

    if dupes != [] do
      raise ArgumentError, "named_sql: duplicate keys in opts: #{inspect(dupes)}"
    end

    :ok
  end

  def assert_keyword_opts!(other) do
    raise ArgumentError, "named_sql expects a keyword list, got: #{inspect(other)}"
  end
end
