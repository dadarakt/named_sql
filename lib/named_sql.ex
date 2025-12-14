defmodule NamedSQL do
  @moduledoc false

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

      # Compile-time checked path (macro): requires literal keyword list
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

  @doc false
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
