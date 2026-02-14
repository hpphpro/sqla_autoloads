from __future__ import annotations

import pytest
import sqlalchemy as sa

from sqla_autoloads.core import SelectBuilder
from sqla_autoloads.node import Node, get_node, init_node

from ..models import Base, Category, Post, User


def _get_node() -> Node:
    try:
        return Node()
    except RuntimeError:
        init_node(get_node(Base))
        return Node()


class TestSelectBuilder:
    def test_build_empty_loads(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=False,
        )
        query = builder.build(loads=())
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "users" in sql.lower()
        # No options should be applied for empty loads
        assert "JOIN" not in sql.upper()

    def test_build_with_nonexistent_key(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=False,
        )
        # Nonexistent key should be silently skipped (bfs returns ())
        query = builder.build(loads=("nonexistent_relationship",))
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "users" in sql.lower()
        assert "JOIN" not in sql.upper()

    def test_build_with_duplicate_loads(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=False,
        )
        # Duplicate keys should not cause duplicate joins
        query = builder.build(loads=("posts", "posts"))
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "users" in sql.lower()

    def test_get_clause_adapter_no_lateral(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=False,
        )
        table = sa.inspect(Post).local_table
        adapter = builder._get_clause_adapter(table)

        assert adapter is None

    def test_get_clause_adapter_with_lateral(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=False,
        )
        # Build with posts to create a lateral map entry
        builder.build(loads=("posts",))

        table = sa.inspect(Post).local_table
        adapter = builder._get_clause_adapter(table)

        assert adapter is not None

    def test_invalid_many_load_warns(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=None,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="invalidload",
            distinct=False,
        )
        with pytest.warns(UserWarning, match="Unknown many_load strategy"):
            builder.build(loads=("posts",))

    def test_distinct_flag(self) -> None:
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=True,
        )
        query = builder.build(loads=("posts",))
        sql = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql.upper()

    def test_invalid_order_by_raises(self) -> None:
        """order_by with nonexistent column raises AttributeError."""
        node = _get_node()
        builder = SelectBuilder(
            model=User,
            node=node,
            limit=50,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=("nonexistent_column",),
            many_load="subqueryload",
            distinct=False,
        )
        with pytest.raises(AttributeError):
            builder.build(loads=("posts",))

    def test_self_ref_without_self_key_raises(self) -> None:
        """Self-ref relationship without self_key raises ValueError."""
        node = _get_node()
        builder = SelectBuilder(
            model=Category,
            node=node,
            limit=None,
            check_tables=False,
            conditions=None,
            self_key="",
            order_by=None,
            many_load="subqueryload",
            distinct=False,
        )
        with pytest.raises(ValueError, match="self_key"):
            builder.build(loads=("children",))
