from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, User

pytestmark = pytest.mark.anyio


class TestOneToMany:
    async def test_basic_load(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3

    async def test_empty_relationship(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        charlie = next(u for u in users if u.name == "charlie")

        assert len(charlie.posts) == 0

    @pytest.mark.lateral
    async def test_lateral_in_sql_with_default_limit(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "LATERAL" in sql_text.upper()

    async def test_no_lateral_with_limit_none(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=None)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "LATERAL" not in sql_text.upper()
