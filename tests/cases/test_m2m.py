from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Role, User

pytestmark = pytest.mark.anyio


class TestManyToMany:
    async def test_basic_load(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("roles",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        role_names = {r.name for r in alice.roles}

        assert role_names == {"admin", "editor"}

    @pytest.mark.lateral
    async def test_secondary_join_in_sql(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("roles",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "user_roles" in sql_text

    async def test_user_without_roles(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("roles",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        charlie = next(u for u in users if u.name == "charlie")

        assert len(charlie.roles) == 0

    async def test_reverse_direction(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Role, loads=("users",))
        result = await session.execute(query)
        roles = result.unique().scalars().all()
        editor = next(r for r in roles if r.name == "editor")
        user_names = {u.name for u in editor.users}

        assert user_names == {"alice", "bob"}
