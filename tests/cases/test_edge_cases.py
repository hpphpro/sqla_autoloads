from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import add_conditions, sqla_select

from ..models import Base, Post, User

pytestmark = pytest.mark.anyio


class TestEdgeCases:
    @pytest.mark.lateral
    async def test_limit_zero(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=0)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0
        for user in users:
            assert len(user.posts) == 0

    @pytest.mark.lateral
    async def test_limit_one(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=1)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) <= 1

    async def test_nonexistent_key_in_loads(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("nonexistent_rel",))
        result = await session.execute(query)
        users = result.scalars().all()

        assert len(users) == 3

    async def test_empty_loads_with_conditions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=(),
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.scalars().all()

        assert len(users) == 3

    async def test_query_with_where_and_loads(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        base_query = sa.select(User).where(User.name == "alice")
        query = sqla_select(model=User, loads=("posts",), query=base_query)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) == 1
        assert users[0].name == "alice"
        assert len(users[0].posts) > 0

    async def test_duplicate_loads_no_crash(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts", "posts"))
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0
