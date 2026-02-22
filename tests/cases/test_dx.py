from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import resolve_col, sqla_laterals, sqla_select, unique_scalars

from ..models import Base, Category, User

pytestmark = pytest.mark.anyio


class TestUniqueScalars:
    async def test_unique_scalars_deduplicates(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",))
        users = unique_scalars(await session.execute(query))

        assert len(users) == 3
        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3

    async def test_unique_scalars_empty_result(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",))
        query = query.where(User.id == -1)
        users = unique_scalars(await session.execute(query))

        assert users == []

    async def test_unique_scalars_no_loads(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sa.select(User)
        users = unique_scalars(await session.execute(query))

        assert len(users) == 3


class TestResolveCol:
    @pytest.mark.lateral
    async def test_resolve_col_basic(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",))
        col = resolve_col(query, "posts.title")

        assert isinstance(col, sa.ColumnElement)

        query = query.where(col == "Alice Post 1")
        users = unique_scalars(await session.execute(query))

        assert len(users) == 1
        assert users[0].name == "alice"

    @pytest.mark.lateral
    async def test_resolve_col_is_alias(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("sent_messages", "received_messages")
        )
        col = resolve_col(query, "messages_received_messages.content")

        assert isinstance(col, sa.ColumnElement)

    @pytest.mark.lateral
    async def test_resolve_col_self_ref(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Category, loads=("children",), self_key="parent_id"
        )
        col = resolve_col(query, "categories_children.name")

        assert isinstance(col, sa.ColumnElement)

    @pytest.mark.lateral
    def test_resolve_col_invalid_alias(self) -> None:
        query = sqla_select(model=User, loads=("posts",))

        with pytest.raises(ValueError, match="not found in query"):
            resolve_col(query, "nonexistent.title")

    @pytest.mark.lateral
    def test_resolve_col_invalid_column(self) -> None:
        query = sqla_select(model=User, loads=("posts",))

        with pytest.raises(ValueError, match="not found in alias"):
            resolve_col(query, "posts.nonexistent")

    def test_resolve_col_bad_format(self) -> None:
        query = sqla_select(model=User, loads=("posts",))

        with pytest.raises(ValueError, match="Expected 'alias.column' format"):
            resolve_col(query, "nodot")


class TestSqlaLaterals:
    @pytest.mark.lateral
    def test_sqla_laterals_returns_aliases(self) -> None:
        query = sqla_select(model=User, loads=("posts", "roles"))
        laterals = sqla_laterals(query)

        assert "posts" in laterals
        assert "roles" in laterals

    def test_sqla_laterals_empty_when_no_limit(self) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=None)
        laterals = sqla_laterals(query)

        assert laterals == {}
