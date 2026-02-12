from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import lru_cache
from typing import Any, TypeVar

import sqlalchemy as sa
from sqlalchemy import orm


T = TypeVar("T", bound=orm.DeclarativeBase)


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
