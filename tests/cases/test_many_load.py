from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, User

pytestmark = pytest.mark.anyio


class TestManyLoad:
    async def test_default_subqueryload(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # Default many_load='subqueryload' with limit=None uses subqueryload.
        query = sqla_select(model=User, loads=("posts",), limit=None)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3

    async def test_selectinload_param(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # many_load='selectinload' with limit=None uses selectinload.
        query = sqla_select(
            model=User, loads=("posts",), limit=None, many_load="selectinload"
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3

    @pytest.mark.lateral
    async def test_many_load_does_not_affect_lateral(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # With limit set, LATERAL is used regardless of many_load.
        query = sqla_select(
            model=User, loads=("posts",), limit=50, many_load="selectinload"
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
