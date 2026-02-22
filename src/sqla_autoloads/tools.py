from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import lru_cache
from typing import Any, TypeVar

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.sql.selectable import Lateral


T = TypeVar("T", bound=orm.DeclarativeBase)
_R = TypeVar("_R")


def unique_scalars(result: sa.Result[tuple[_R]]) -> Sequence[_R]:
    """Shorthand for ``result.unique().scalars().all()``.

    Use after ``session.execute(query)`` on queries built by :func:`sqla_select`
    to deduplicate rows produced by outer-join eager loading.

    Example (async)::

        users = unique_scalars(await session.execute(query))

    Example (sync)::

        users = unique_scalars(session.execute(query))
    """
    return result.unique().scalars().all()


@lru_cache
def _get_primary_key(model: type[T]) -> sa.ColumnElement[Any]:
    """Return the first primary-key column element for *model* (cached)."""
    return next(iter(model.__table__.primary_key))


@lru_cache
def _get_table_name(model: type[T]) -> str:
    """Return the table name for *model*, preferring ``__tablename__`` (cached)."""
    result = getattr(
        model,
        "__tablename__",
        model.__table__.description,
    )
    if not result:
        raise ValueError(f"Cannot determine tablename for {model}")

    return result


def get_table_name(model: type[T]) -> str:
    """Get the table name for a SQLAlchemy model.

    Args:
        model: SQLAlchemy model class.

    Returns:
        The table name as a string.

    Raises:
        ValueError: If the table name cannot be determined.
    """
    return _get_table_name(model)


def get_primary_key(model: type[T]) -> sa.ColumnElement[Any]:
    """Get the primary key column for a SQLAlchemy model.

    Args:
        model: SQLAlchemy model class.

    Returns:
        The primary key column element.
    """
    return _get_primary_key(model)


def get_table_names(query: sa.Select[tuple[T]]) -> Sequence[str]:
    """Extract all table names from a SQLAlchemy select query.

    This function traverses the query's FROM clause to identify all tables,
    including those in joins and aliases.

    Args:
        query: SQLAlchemy select query.

    Returns:
        Sequence of table names found in the query.
    """
    seen: set[str] = set()
    out: list[str] = []

    def add(name: str | None) -> None:
        if name and name not in seen:
            seen.add(name)
            out.append(name)

    for root in query.get_final_froms():
        stack: list[Any] = [root]
        while stack:
            node = stack.pop()

            if isinstance(node, sa.Table):
                add(node.name)
                continue

            if isinstance(node, sa.Join):
                stack.extend([node.left, node.right])
                continue

            add(getattr(node, "name", None))
            if hasattr(node, "element"):
                stack.append(node.element)

    return out


def add_conditions(
    *conditions: sa.ColumnExpressionArgument[bool],
) -> Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]]:
    """Create a function that adds WHERE conditions to a select query.

    This is a helper function for creating condition functions that can be used
    with the sqla_select function's conditions parameter.

    Args:
        *conditions: SQLAlchemy column expressions that evaluate to boolean.

    Returns:
        A function that takes a select query and returns it with added conditions.

    Example:
        >>> condition_func = add_conditions(Role.active == True, Role.level > 3)
        >>> # Use with sqla_select:
        >>> query = sqla_select(
        ...     model=User, loads=("roles",), conditions={"roles": condition_func}
        ... )
    """

    def _add(query: sa.Select[tuple[T]]) -> sa.Select[tuple[T]]:
        return query.where(*conditions)

    return _add


def _find_from_by_name(
    root: sa.FromClause, name: str
) -> sa.FromClause | None:
    """Find a subquery/alias/lateral/table with *name* in the FROM tree (iterative)."""
    stack: list[sa.FromClause] = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, sa.Join):
            stack.append(node.left)
            stack.append(node.right)
            continue
        if getattr(node, "name", None) == name:
            return node
        element = getattr(node, "element", None)
        if element is not None:
            stack.append(element)
    return None


def resolve_col(query: sa.Select[Any], ref: str) -> sa.ColumnElement[Any]:
    """Resolve ``'alias.column'`` to a bound ColumnElement from *query*.

    Works on queries built by :func:`sqla_select`. Instead of
    ``sa.literal_column("posts.title")``, use::

        col = resolve_col(query, "posts.title")
        query = query.where(col == "hello")

    The *ref* format is ``alias_name.column_name`` where ``alias_name`` is
    the LATERAL/subquery alias (e.g. ``posts``, ``messages_received_messages``,
    ``categories_children``).  These are SQL identifiers — never dotted paths.
    Use ``sqla_laterals(query)`` or ``print(query)`` to discover alias names.

    Raises ``ValueError`` if alias or column not found.
    """
    alias_name, sep, col_name = ref.partition(".")
    if not sep:
        raise ValueError(f"Expected 'alias.column' format, got {ref!r}")
    for root in query.get_final_froms():
        found = _find_from_by_name(root, alias_name)
        if found is not None and hasattr(found, "c"):
            try:
                return found.c[col_name]
            except KeyError:
                raise ValueError(
                    f"Column {col_name!r} not found in alias {alias_name!r}. "
                    f"Available: {[c.key for c in found.c]}"
                ) from None
    raise ValueError(
        f"Alias {alias_name!r} not found in query. "
        f"Available: {get_table_names(query)}"
    )


def sqla_laterals(query: sa.Select[Any]) -> dict[str, sa.Subquery]:
    """Return ``{alias_name: subquery}`` for all LATERAL joins in *query*."""
    out: dict[str, sa.Subquery] = {}
    for root in query.get_final_froms():
        stack: list[sa.FromClause] = [root]
        while stack:
            node = stack.pop()
            if isinstance(node, sa.Join):
                stack.append(node.left)
                stack.append(node.right)
                continue
            if isinstance(node, Lateral):
                name = getattr(node, "name", None)
                if name:
                    out[name] = node
    return out
