defmodule NamedSQL do
  @moduledoc false

  # Reserved DSL option keys (atoms, fixed at compile-time)
  @reserved_keys [:result_mapper, :timeout, :log]
  @reserved_key_strings Enum.map(@reserved_keys, &Atom.to_string/1)

  defmacro __using__(opts) do
    repo =
      Keyword.get(opts, :repo) ||
        raise ArgumentError, "`:repo` is a required option: `use NamedSQL, repo: <Your Repo>`"

    quote do
      @named_query_repo unquote(repo)

      defmacro named_query(query_ast, opts_ast) do
        NamedSQL.__named_query_macro__(@named_query_repo, query_ast, opts_ast, __CALLER__)
      end
    end
  end

  def __named_query_macro__(repo, query_ast, opts_ast, caller) do
    query = Macro.expand(query_ast, caller)
    expanded_opts = Macro.expand(opts_ast, caller)

    unless is_binary(query) do
      raise ArgumentError,
        "named_query/2 expects a literal binary query at compile time, got: #{Macro.to_string(query_ast)}"
    end

    case expanded_opts do
      opts when is_list(opts) ->
        :ok
      _ ->
        raise ArgumentError,
          "named_query/2 expects a keyword list as the second parameter. Use: named_query(sql, key: value, ...)"
    end

    param_regex = ~r/\$(?<name>[A-Za-z_][A-Za-z0-9_-]*)/

    extracted_names =
      Regex.scan(param_regex, query, capture: :all_but_first)
      |> List.flatten()

    bad_reserved =
      extracted_names
      |> Enum.filter(&(&1 in @reserved_key_strings))

    if bad_reserved != [] do
      raise ArgumentError,
        "named_query: query uses reserved parameter names: #{inspect(bad_reserved)}. " <>
        "Reserved: #{inspect(@reserved_keys)}"
    end

    # mapping: name(string) -> index(int)
    {mapping, _} =
      Enum.reduce(extracted_names, {%{}, 1}, fn(name, {acc, idx}) ->
        if Map.has_key?(acc, name), do: {acc, idx}, else: {Map.put(acc, name, idx), idx + 1}
      end)

    normalized_query =
      Enum.reduce(mapping, query, fn({name, idx}), acc ->
        String.replace(acc, "$#{name}", "$#{idx}")
      end)

    expected_param_keys = Map.keys(mapping)
    ordered_param_keys =
      mapping
      |> Enum.sort_by(fn({_name, idx}) -> idx end)
      |> Enum.map(fn({name, _idx}) -> name end)

    case literal_keyword_keys(opts_ast, caller) do
      {:ok, provided_keys_atoms} ->
        provided_param_key_strings =
          provided_keys_atoms
          |> Enum.reject(&(&1 in @reserved_keys))
          |> Enum.map(&Atom.to_string/1)

        dupes =
          provided_param_key_strings
          |> Enum.frequencies()
          |> Enum.filter(fn({_k, n}) -> n > 1 end)
          |> Enum.map(&elem(&1, 0))

        if dupes != [] do
          raise ArgumentError,
            "named_query: duplicate parameter keys in keyword list: #{inspect(dupes)}"
        end

        missing = expected_param_keys -- provided_param_key_strings

        if missing != [] do
          raise ArgumentError,
            "named_query: missing keys in params: #{inspect(missing)}. " <>
            "Expected: #{inspect(expected_param_keys)}, provided: #{inspect(provided_param_key_strings)}"
        end

        additional = provided_param_key_strings -- expected_param_keys

        if additional != [] do
          raise ArgumentError,
            "named_query: additional keys in params: #{inspect(additional)}. " <>
            "Expected: #{inspect(expected_param_keys)}, provided: #{inspect(provided_param_key_strings)}"
        end

      :dynamic ->
        raise ArgumentError,
          "named_query/2 requires a *literal* keyword list for compile-time checks " <>
          "For dynamic params, use named_query_dynamic/2."
    end

    quote do
      opts = unquote(opts_ast)
      result_mapper = Keyword.get(opts, :result_mapper)

      params_by_string =
        Keyword.drop(opts, unquote(@reserved_keys))
        |> Map.new(fn({k, v}) -> {Atom.to_string(k), v} end)

      ordered_params =
        unquote(ordered_param_keys)
        |> Enum.map(fn(key_string) ->
          Map.fetch!(params_by_string, key_string)
        end)

      NamedSQL.runtime(unquote(repo), unquote(normalized_query), ordered_params, result_mapper)
    end
  end

  def named_query_dynamic(repo, query, opts) when is_binary(query) do
    assert_keyword_opts!(opts)

    param_regex = ~r/\$(?<name>[A-Za-z_][A-Za-z0-9_-]*)/

    extracted_names =
      Regex.scan(param_regex, query, capture: :all_but_first)
      |> List.flatten()

    bad_reserved = Enum.filter(extracted_names, &(&1 in @reserved_key_strings))
    if bad_reserved != [] do
      raise ArgumentError, "named_query_dynamic: query uses reserved parameter names: #{inspect(bad_reserved)}"
    end

    {mapping, _} =
      Enum.reduce(extracted_names, {%{}, 1}, fn(name, {acc, idx}) ->
        if Map.has_key?(acc, name), do: {acc, idx}, else: {Map.put(acc, name, idx), idx + 1}
      end)

    normalized_query =
      Enum.reduce(mapping, query, fn({name, idx}), acc ->
        String.replace(acc, "$#{name}", "$#{idx}")
      end)

    expected = Map.keys(mapping)
    ordered  =
      mapping
      |> Enum.sort_by(fn({_name, idx}) -> idx end)
      |> Enum.map(fn({name, _}) -> name end)

    result_mapper = Keyword.get(opts, :result_mapper)
    params_kw     = Keyword.drop(opts, @reserved_keys)

    params_by_string = Map.new(params_kw, fn({k, v}) -> {Atom.to_string(k), v} end)

    missing = Enum.reject(expected, &Map.has_key?(params_by_string, &1))
    if missing != [] do
      raise ArgumentError, "named_query_dynamic: missing keys in params: #{inspect(missing)}"
    end

    additional = Map.keys(params_by_string) -- expected
    if additional != [] do
      raise ArgumentError, "named_query_dynamic: additional keys in params: #{inspect(additional)}"
    end

    ordered_params = Enum.map(ordered, &Map.fetch!(params_by_string, &1))

    runtime(repo, normalized_query, ordered_params, result_mapper)
  end


  defp literal_keyword_keys(opts_ast, caller) do
    expanded = Macro.expand(opts_ast, caller)

    if is_list(expanded) and Keyword.keyword?(expanded) do
      {:ok, Enum.map(expanded, fn({k, _v}) -> k end)}
    else
      :dynamic
    end
  end

  def runtime(repo, normalized_query, params, result_mapper) do
    with {:ok, %{rows: rows, columns: columns}} <- repo.query(normalized_query, params) do
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

  def assert_keyword_opts!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "named_query expects a keyword list, got: #{inspect(opts)}"
    end

    dupes =
      opts
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(fn({_k, n}) -> n > 1 end)
      |> Enum.map(&elem(&1, 0))

    if dupes != [] do
      raise ArgumentError, "duplicate keys in opts: #{inspect(dupes)}"
    end

    :ok
  end

  def assert_keyword_opts!(other) do
    raise ArgumentError, "named_query expects a keyword list, got: #{inspect(other)}"
  end
end

