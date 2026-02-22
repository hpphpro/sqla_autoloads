# FAQ — LATERAL alias naming & `.where()` filtering

When `sqla_select` builds a query with `limit` (default `50`), it wraps each one-to-many / many-to-many relationship in a **LATERAL subquery**. The subquery gets an alias name that **replaces** the original table name in the `FROM` clause.

In most cases `Model.column` works as expected — when the LATERAL alias matches the table name (e.g. `posts`), SQLAlchemy resolves `Post.title` to `posts.title` and everything is fine. Problems appear only when the alias **diverges** from the original table name: `{table}_{relkey}` or `{table}_alias`.

---

## 1. When does `.where(Model.column)` work and when doesn't it?

**Works** — first occurrence of a table, alias = table name:

```python
query = sqla_select(model=User, loads=("posts",))

# LATERAL alias is "posts" — same as the table name, so Post.title resolves correctly
query = query.where(Post.title == "hello")  # OK
```

**Doesn't work** — alias diverges from table name:

```python
# Self-referential → alias is "categories_children", not "categories"
query = sqla_select(model=Category, loads=("children",), self_key="parent_id")
query = query.where(Category.name == "Electronics")          # WRONG — resolves to "categories.name"
query = query.where(sa.literal_column("categories_children.name") == "Electronics")  # CORRECT
# Or use resolve_col (recommended):
col = resolve_col(query, "categories_children.name")
query = query.where(col == "Electronics")

# Multiple FKs → second+ alias is "messages_received_messages"
query = sqla_select(model=User, loads=("sent_messages", "received_messages"))
query = query.where(sa.literal_column("messages_received_messages.id") > 10)  # CORRECT
# Or: resolve_col(query, "messages_received_messages.id")
```

**Tip:** use `sqla_laterals(query)` or `print(query)` to see alias names.

---

## 2. Self-referential relationships (Category → parent / children)

Self-referential LATERAL aliases always use the format `{table}_{relationship_key}`:

```python
query = sqla_select(
    model=Category,
    loads=("children",),
    self_key="parent_id",
)
```

LATERAL alias: **`categories_children`**

```python
# Using resolve_col (recommended):
col = resolve_col(query, "categories_children.name")
query = query.where(col == "Electronics")

# Or using literal_column:
query = query.where(sa.literal_column("categories_children.name") == "Electronics")
```

---

## 3. Multiple FKs to the same table (Message → from_user, to_user, owner)

When several relationships point to the same target table, the **first** one gets the plain table name, and all subsequent ones get `{table}_{relationship_key}`:

```python
query = sqla_select(
    model=User,
    loads=("sent_messages", "received_messages", "owned_messages"),
)
```

| Order | Relationship         | LATERAL alias                  |
|-------|----------------------|--------------------------------|
| 1st   | `sent_messages`      | `messages`                     |
| 2nd   | `received_messages`  | `messages_received_messages`   |
| 3rd   | `owned_messages`     | `messages_owned_messages`      |

```python
# Using resolve_col (recommended):
col = resolve_col(query, "messages.id")
query = query.where(col > 10)

col2 = resolve_col(query, "messages_received_messages.id")
query = query.where(col2 > 10)

# Or using literal_column:
query = query.where(sa.literal_column("messages.id") > 10)
query = query.where(sa.literal_column("messages_received_messages.id") > 10)
```

The order depends on which relationship target class is encountered first during query construction.

---

## 4. `check_tables=True` and collision with the base query

If you pass a base query that already contains a table, and `sqla_select` tries to create a LATERAL alias with the same name, it appends `_alias`:

```python
base = sa.select(User).outerjoin(Post, User.id == Post.author_id)
query = sqla_select(
    model=User,
    loads=("posts",),
    query=base,
    check_tables=True,
)
```

Without `check_tables=True` the LATERAL name `posts` would collide with the already-joined `posts` table. With it enabled:

LATERAL alias: **`posts_alias`**

```python
# Using resolve_col (recommended):
col = resolve_col(query, "posts_alias.title")
query = query.where(col == "hello")

# Or using literal_column:
query = query.where(sa.literal_column("posts_alias.title") == "hello")
```

---

## 5. Per-relationship `order_by` and `limit` override via `conditions`

The `conditions` parameter accepts any `Callable[[sa.Select], sa.Select]` — not just WHERE filters. Since conditions are applied **after** the default `limit` and `order_by`, you can override them per-relationship:

**Override order_by:**

```python
query = sqla_select(
    model=User,
    loads=("posts",),
    conditions={
        # Reset default ordering, apply custom
        "posts": lambda q: q.order_by(None).order_by(Post.created_at.desc()),
    },
)
```

**Override limit:**

```python
query = sqla_select(
    model=User,
    loads=("posts", "roles"),
    limit=50,
    conditions={
        # posts — only 5 most recent
        "posts": lambda q: q.limit(None).limit(5).order_by(None).order_by(Post.created_at.desc()),
        # roles — no override, keeps default limit=50
    },
)
```

**Combine with filtering:**

```python
query = sqla_select(
    model=User,
    loads=("posts",),
    conditions={
        "posts": lambda q: (
            q.where(Post.is_published == True)
             .order_by(None)
             .order_by(Post.created_at.desc())
             .limit(None)
             .limit(10)
        ),
    },
)
```

> `add_conditions(...)` is a shortcut that only adds `.where()`. For `order_by` / `limit` overrides, use a lambda directly.

---

## 6. Quick reference

| Scenario                       | LATERAL alias name                | Example                         |
|--------------------------------|-----------------------------------|---------------------------------|
| First occurrence of table      | `{table}`                         | `posts`                         |
| Same table loaded again        | `{table}_{relationship_key}`      | `messages_received_messages`    |
| Self-referential relationship  | `{table}_{relationship_key}`      | `categories_children`           |
| `check_tables=True` + collision| `{table}_alias` or `{table}_{relationship_key}_alias` | `posts_alias` |

---

## 7. How do I find the exact alias name?

Use `sqla_laterals` to get a dict of all LATERAL aliases:

```python
from sqla_autoloads import sqla_laterals

query = sqla_select(model=User, loads=("posts", "roles"))
print(sqla_laterals(query))  # {"posts": <Lateral ...>, "roles": <Lateral ...>}
```

Or print the compiled query:

```python
# Quick look
print(query)

# With bound parameters rendered
from sqlalchemy.dialects import postgresql
print(query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
```

Look for `LATERAL (...) AS <name>` in the output — `<name>` is the alias to use with `resolve_col()` or `sa.literal_column()`.

---

## 8. ZIP optimization & performance

### What is ZIP?

When a query loads 2+ **uselist** (one-to-many or many-to-many) relationships at the **same depth**, each LATERAL subquery returns up to `limit` rows. Without ZIP, the database produces a cross-product between siblings — e.g. 50 posts × 50 roles = 2,500 rows per user.

ZIP eliminates this by assigning a shared **ROW_NUMBER** series (via a recursive CTE) to sibling LATERAL subqueries at each depth. Each sibling filters on the same RN range, so they align row-by-row instead of multiplying.

### When does it activate?

ZIP activates automatically when **all** of these conditions are met:

1. `limit` is set (not `None`)
2. Two or more **uselist** relationships exist at the same depth level
3. The relationships are loaded via LATERAL subqueries

Single relationships at a given depth, or `limit=None` queries, are unaffected.

To disable ZIP entirely, pass `optimization=False` to `sqla_select`. All sibling LATERALs will join on `ON TRUE` (cross-product), which may be faster for small datasets.

### Row count formula

| Without ZIP | With ZIP |
|---|---|
| `limit ^ number_of_laterals` | `limit ^ number_of_zip_depths` |

Example with `limit=50`, loads `("posts.comments", "posts.tags", "roles")`:

- **3 LATERAL subqueries**: `roles` (depth 1), `comments` (depth 2), `tags` (depth 2)
- **Without ZIP**: 50 × 50 × 50 = 125,000 rows per user
- **With ZIP**: depth 1 has 1 lateral (`roles`, no zip needed), depth 2 has 2 siblings (`comments` + `tags`, zipped) → 50 × 50 = 2,500 rows per user

### Performance note

The ZIP query uses a recursive CTE + ROW_NUMBER window function. For **small datasets** (few children per relationship), the overhead of the CTE can make the single complex query slower than issuing separate simple queries. The benefit appears at scale — roughly **50+ children per relationship** — where the cross-product elimination dominates.
