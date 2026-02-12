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

# Multiple FKs → second+ alias is "messages_received_messages"
query = sqla_select(model=User, loads=("sent_messages", "received_messages"))
query = query.where(sa.literal_column("messages_received_messages.id") > 10)  # CORRECT
```

**Tip:** `print(query)` to see the actual SQL and alias names.

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
# First relationship — plain table name, Message.id works too
query = query.where(sa.literal_column("messages.id") > 10)

# Second relationship — table + relkey, must use literal_column
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

Print the compiled query:

```python
query = sqla_select(model=User, loads=("posts", "roles"))

# Quick look
print(query)

# With bound parameters rendered
from sqlalchemy.dialects import postgresql
print(query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
```

Look for `LATERAL (...) AS <name>` in the output — `<name>` is the alias to use with `sa.literal_column()`.
