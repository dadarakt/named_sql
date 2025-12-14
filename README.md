# NamedSQL

[![Hex.pm](https://img.shields.io/hexpm/v/named_sql.svg)](https://hex.pm/packages/named_sql)
[![License](https://img.shields.io/hexpm/l/named_sql.svg)](LICENSE)

# NamedSQL

**NamedSQL** is a small Elixir library for writing **plain SQL with named parameters**, with **compile-time validation** and **no DSL**.

You write SQL.
NamedSQL checks your parameters.
You get a nicely mapped output.
That’s it.

---

## Why NamedSQL?

It was born out of reoccurring frustration of having to write complex SQL queries using Ecto's DSL.
While there are advantages to having composable queries, SQL in itself often the perfect language to describe complex relations,
especially when using a number of CTEs in your queries.

Ecto allows you to use raw sql (`MyApp.Repo.query/3`) as an esacpe hatch, but it is not treated as first class citizen.
Also, the code tends to be hard to maintain, as you write queries with numbered parameters (`$1`, `$2`, etc...).
- Harder to read existing queries
- Easy to make order mistakes, especially when changing code


The NamedSQL approach:

- **SQL is a good fit** to write complex queries
- **Named parameters** to those queries make it readable and avoid order problems when changes are made
- **Compile-time checks** catch mistakes early
- **No ORM semantics**, no query builders, no magic

If you like writing SQL and want it to be safer and cleaner in Elixir, this library is for you.

---

## Installation

Add `named_sql` to your dependencies:

```elixir
defp deps do
  [
    {:named_sql, "~> 0.1.0"}
  ]
end
```

---

##Setup

Use NamedSQL inside your Repo module:

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres

  use NamedSQL, repo: __MODULE__
end
```

---

## Usage

Because `named_sql/2` is a macro (to enable compile-time checks), you must require the repo at the callsite:

```elixir
alias MyApp.Repo
require Repo
```

### Basic example

```elixir
Repo.named_sql("""
  SELECT name, birth
  FROM users
  WHERE name = $name
  """,
  name: "Jürgen"
)
```

### Result
```elixir
[
  %{"name" => "Jürgen", "birth" => ~N[2025-12-09 13:03:12]}
]
```

Results are string-keyed maps by default, to avoid runtime atom creation.

---

## Named Parameters

SQL placeholders use `$` followed by an identifier:

```sql
WHERE user_id = $user_id AND created_at > $since
```

Parameters are passed as a keyword list:

```eliixir
Repo.named_sql(sql,
  user_id: 42,
  since: ~N[2025-01-01 00:00:00]
)
```

### Validation

This library provides compile-time validation for:
- **Missing** parameters that appear in the query but not in the parameters
- **Additional** parameters that do not appear in the query but in the parameters
- **Duplicate** parameters
- **Reserved keyword** parameters which are passed through to Ecto

Compile-time validation is only applied when a literal keyword map is presented for the parameters.
Dynamic keyword lists will lead to runtime errors instead.

---

## Result Mapping

You can use the `:result_mapper` option to control how result rows are formatted, to avoid the
intermediate map format in case it's not desired.

The mapper receives each row as a list, in column order.

```elixir
Repo.named_sql("""
  SELECT name, birth
  FROM users
  """,
  result_mapper: fn [name, birth] ->
    %{name: name, birth: birth}
  end
)
```

This is the recommended way to return structs or atom-keyed maps.

---

## Reserved options

The following option keys are reserved and cannot be used as SQL parameters:
- `:result_mapper` -> To map output (see previous section)
- `:timeout`, `:log` -> Options for Ecto, see `Ecto.Repo.query/3` for more information

Using them in the SQL query will raise an error.

## Design principles
NamedSQL does not try to replace Ecto or build a query language.
It simply makes raw SQL safer and nicer to use.

- Not a DSL, just a simple tool
- No runtime atom creation
- As much compile-time verification as is reasonable for a dynamic language
- Minimal surface area of macro

---

License MIT

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/named_sql>.

