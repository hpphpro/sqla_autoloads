# sqla-autoloads

[![CI](https://github.com/hpphpro/sqla_autoloads/actions/workflows/test.yaml/badge.svg)](https://github.com/hpphpro/sqla_autoloads/actions/workflows/test.yaml)
[![PyPI version](https://badge.fury.io/py/sqla-autoloads.svg)](https://badge.fury.io/py/sqla-autoloads)
[![Python](https://img.shields.io/pypi/pyversions/sqla_autoloads)](https://pypi.org/project/sqla-autoloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Automatic relationship eager-loading for SQLAlchemy. Build `SELECT` queries with LATERAL subqueries, dotted-path traversal, self-referential handling, and per-relationship conditions — all from a single function call.

## Install

```bash
pip install sqla-autoloads
```

## Quick Start

```python
from sqla_autoloads import init_node, get_node, sqla_select

# 1. Initialize once at startup
init_node(get_node(Base))

# 2. Query with eager-loading
query = sqla_select(model=User, loads=("posts", "roles"))
result = await session.execute(query)
users = result.unique().scalars().all()
# users[0].posts  — already loaded, no N+1
```

## Why?

Loading relationships in SQLAlchemy requires writing boilerplate joins and loader options for every combination of relationships. This grows quickly — especially with optional loads, LATERAL limits, and deep chains.

**Raw SQLAlchemy** — manual `if` per relationship:

```python
UserLoad = Literal["posts", "roles", "profile"]

async def get_users(*loads: UserLoad) -> list[User]:
    query = sa.select(User)
    options = []
    if "posts" in loads:
        lateral = (
            sa.select(Post)
            .where(Post.author_id == User.id)
            .order_by(Post.id.desc())
            .limit(50)
            .lateral()
        )
        query = query.outerjoin(lateral, sa.true())
        options.append(orm.contains_eager(User.posts, alias=lateral))
    if "roles" in loads:
        query = query.outerjoin(user_roles).outerjoin(Role)
        options.append(orm.contains_eager(User.roles))
    if "profile" in loads:
        query = query.outerjoin(Profile)
        options.append(orm.contains_eager(User.profile))
    if options:
        query = query.options(*options)
    return (await session.execute(query)).unique().scalars().all()
```

**sqla-autoloads** — one call, on-demand:

```python
UserLoad = Literal["posts", "roles", "profile"]

async def get_users(*loads: UserLoad) -> list[User]:
    query = sqla_select(model=User, loads=loads)
    return (await session.execute(query)).unique().scalars().all()
```

## Features

- **LATERAL subqueries** with configurable limit (default 50 per relationship)
- **Dotted paths** — `"posts.comments.reactions"` traverses the chain automatically
- **Self-referential relationships** — parent/children on the same model
- **M2M** through association tables
- **Multiple FKs** to the same table (e.g. `from_user`, `to_user`, `owner`)
- **Per-relationship conditions** via `add_conditions`
- **Automatic strategy selection** — `contains_eager`, `selectinload`, `joinedload`, `subqueryload`
- **LRU-cached** query construction — same parameters return the same compiled query
- **Async-first**, sync-compatible
- **Fully typed** (PEP 561 `py.typed` marker)

## Usage

### Basic loads

```python
# One-to-many
query = sqla_select(model=User, loads=("posts",))

# Many-to-many
query = sqla_select(model=User, loads=("roles",))

# Many-to-one / one-to-one
query = sqla_select(model=Post, loads=("author",))

# Multiple relationships at once
query = sqla_select(model=User, loads=("posts", "roles", "profile"))
```

### Deep / dotted paths

```python
# Load posts → comments → reactions in one query
query = sqla_select(model=User, loads=("posts.comments.reactions",))
```

### Conditions

```python
from sqla_autoloads import add_conditions

query = sqla_select(
    model=User,
    loads=("posts", "roles"),
    conditions={
        "roles": add_conditions(Role.level > 3),
    },
)
```

### Limit and ordering

```python
# Custom limit per relationship (default is 50)
query = sqla_select(model=User, loads=("posts",), limit=10)

# No limit — load all related rows (uses subqueryload/selectinload)
query = sqla_select(model=User, loads=("posts",), limit=None)

# Custom ordering
query = sqla_select(model=User, loads=("posts",), order_by=("title",))
```

### Self-referential

```python
query = sqla_select(
    model=Category,
    loads=("children", "parent"),
    self_key="parent_id",
)
```

### Extending an existing query

```python
base = sa.select(User).where(User.active == True)
query = sqla_select(model=User, loads=("posts",), query=base)
```

### Many-load strategy

```python
# Use selectinload instead of default subqueryload for limit=None
query = sqla_select(model=User, loads=("posts",), limit=None, many_load="selectinload")
```

## Important notes

### LATERAL support — database compatibility

The `limit` parameter (default `50`) uses **LATERAL subqueries** to cap the number of related rows per parent. LATERAL is supported by **PostgreSQL** and **MySQL 8.0+** only.

**SQLite, MariaDB, and MSSQL do not support LATERAL.** On these databases, pass `limit=None` to disable LATERAL and fall back to `subqueryload`/`selectinload`:

```python
query = sqla_select(model=User, loads=("posts",), limit=None)
```

### `.unique()` is required on results

`sqla_select` uses `outerjoin` + `contains_eager` and `joinedload` to load relationships. These strategies produce **duplicate parent rows** in the raw result (one row per related object). This is standard SQLAlchemy behavior.

You **must** call `.unique()` on the result before `.scalars()`:

```python
result = await session.execute(query)
users = result.unique().scalars().all()
```

Without `.unique()`, SQLAlchemy will raise an error or return duplicate objects.

> **Note:** The `distinct=True` parameter in `sqla_select` applies SQL-level `DISTINCT`, which deduplicates by column values. It does **not** replace `.unique()`, which deduplicates by object identity.

### Filtering with `.where()` after `sqla_select`

When `sqla_select` uses LATERAL subqueries, table names in the FROM clause become LATERAL aliases. This means `Model.column` references may not resolve correctly in `.where()` — use `sa.literal_column("alias.column")` instead. See **[FAQ.md](FAQ.md)** for naming rules and examples.

## API Reference

| Function | Description |
|---|---|
| `sqla_select(**params)` | Build a SELECT with eager-loaded relationships |
| `init_node(mapping)` | Initialize the relationship graph singleton (call once at startup) |
| `get_node(Base)` | Extract relationship mapping from a declarative base |
| `add_conditions(*exprs)` | Create a condition function for filtering loaded relationships |
| `Node()` | Access the singleton relationship graph |
| `SelectBuilder` | Query builder class (used internally, also exported) |

### `sqla_select` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `type[T]` | *required* | SQLAlchemy model class |
| `loads` | `tuple[str, ...]` | `()` | Relationship paths to eager-load |
| `limit` | `int \| None` | `50` | LATERAL limit per relationship (`None` = no limit) |
| `conditions` | `Mapping[str, Callable]` | `None` | Per-relationship WHERE conditions |
| `order_by` | `tuple[str, ...]` | `None` | Column names for ordering (default: PK desc) |
| `query` | `sa.Select` | `None` | Existing query to extend |
| `self_key` | `str` | auto-detected | FK column for self-referential relationships |
| `many_load` | `str` | `"subqueryload"` | Strategy for `limit=None`: `"subqueryload"` or `"selectinload"` |
| `distinct` | `bool` | `False` | Apply DISTINCT to the query |
| `check_tables` | `bool` | `False` | Check for existing tables to avoid duplicate joins |

## Requirements

- Python 3.10+
- SQLAlchemy 2.0+

## License

[MIT](LICENSE)
