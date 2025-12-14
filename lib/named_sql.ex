defmodule NamedSQL do
  @moduledoc false

  # Reserved DSL option keys (atoms, fixed at compile-time)
  @reserved_keys [:result_mapper, :as, :timeout]
  @reserved_key_strings Enum.map(@reserved_keys, &Atom.to_string/1)

  defmacro __using__(opts) do
    repo =
      Keyword.get(opts, :repo) ||
        raise ArgumentError, "use NamedSQL, repo: __MODULE__ is required"

    quote do
      @named_query_repo unquote(repo)

      defmacro named_query(query_ast, opts_ast) do
        NamedSQL.__named_query_macro__(@named_query_repo, query_ast, opts_ast, __CALLER__)
      end
    end
  end

  # ---- macro implementation (compile-time work) ----------------------------

  def __named_query_macro__(repo, query_ast, opts_ast, caller) do
    query = Macro.expand(query_ast, caller)
    expanded_opts = Macro.expand(opts_ast, caller)

    unless is_binary(query) do
      raise ArgumentError,
            "named_query/2 expects a literal binary query at compile time, got: #{Macro.to_string(query_ast)}"
    end

    # compile-time: reject map literal opts
    case expanded_opts do
      {:%{}, _, _} ->
        raise ArgumentError,
              "named_query/2 expects a keyword list, not a map. Use: named_query(sql, key: value, ...)"

      _ ->
        :ok
    end

    param_regex = ~r/\$(?<name>[A-Za-z_][A-Za-z0-9_-]*)/

    # extracted_names are STRINGS (no atoms created from SQL)
    extracted_names =
      Regex.scan(param_regex, query, capture: :all_but_first)
      |> List.flatten()

    # compile-time: forbid reserved keys in query placeholders
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
      Enum.reduce(extracted_names, {%{}, 1}, fn name, {acc, idx} ->
        if Map.has_key?(acc, name), do: {acc, idx}, else: {Map.put(acc, name, idx), idx + 1}
      end)

    normalized_query =
      Enum.reduce(mapping, query, fn {name, idx}, acc ->
        String.replace(acc, "$#{name}", "$#{idx}")
      end)

    expected_param_keys = Map.keys(mapping)              # strings
    ordered_param_keys =
      mapping
      |> Enum.sort_by(fn {_name, idx} -> idx end)
      |> Enum.map(fn {name, _idx} -> name end)           # strings

    # compile-time missing/additional-key detection if opts literal
    case literal_keyword_keys(opts_ast, caller) do
      {:ok, provided_keys_atoms} ->
        provided_param_key_strings =
          provided_keys_atoms
          |> Enum.reject(&(&1 in @reserved_keys))
          |> Enum.map(&Atom.to_string/1)

        dupes =
          provided_param_key_strings
          |> Enum.frequencies()
          |> Enum.filter(fn {_k, n} -> n > 1 end)
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
        :ok
    end

    quote do
      opts = unquote(opts_ast)

      NamedSQL.assert_keyword_opts!(opts)

      result_mapper = Keyword.get(opts, :result_mapper)
      params_kw = Keyword.drop(opts, unquote(@reserved_keys))

      # Build a string-keyed map from the keyword params (no new atoms)
      params_by_string =
        Map.new(params_kw, fn {k, v} -> {Atom.to_string(k), v} end)

      # Runtime: check for missing/additional keys too (for dynamic opts)
      expected = unquote(expected_param_keys)

      missing =
        expected
        |> Enum.reject(&Map.has_key?(params_by_string, &1))

      if missing != [] do
        raise ArgumentError,
              "named_query: missing keys in params: #{inspect(missing)}. Expected: #{inspect(expected)}"
      end

      additional =
        Map.keys(params_by_string) -- expected

      if additional != [] do
        raise ArgumentError,
              "named_query: additional keys in params: #{inspect(additional)}. Expected: #{inspect(expected)}"
      end

      ordered_params =
        unquote(ordered_param_keys)
        |> Enum.map(fn key_string ->
          Map.fetch!(params_by_string, key_string)
        end)

      NamedSQL.runtime(unquote(repo), unquote(normalized_query), ordered_params, result_mapper)
    end
  end

  defp literal_keyword_keys(opts_ast, caller) do
    expanded = Macro.expand(opts_ast, caller)

    if is_list(expanded) and Keyword.keyword?(expanded) do
      {:ok, Enum.map(expanded, fn {k, _v} -> k end)}
    else
      :dynamic
    end
  end

  # ---- runtime (single place) ----------------------------------------------

  def runtime(repo, normalized_query, params, result_mapper) do
    with {:ok, %{rows: rows, columns: columns}} <- repo.query(normalized_query, params) do
      map_fun =
        case result_mapper do
          nil ->
            fn row ->
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
      |> Enum.filter(fn {_k, n} -> n > 1 end)
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

