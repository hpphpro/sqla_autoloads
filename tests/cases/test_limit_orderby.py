from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Category, User

pytestmark = [pytest.mark.anyio, pytest.mark.lateral]


class TestLimitAndOrderBy:
    async def test_default_limit_50(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "50" in sql_text

    async def test_custom_limit(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=2)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) <= 2

    async def test_limit_none_no_lateral(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=None)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "LATERAL" not in sql_text.upper()
        assert "LIMIT" not in sql_text.upper()

    async def test_order_by_custom(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), order_by=("title",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "title" in sql_text

    async def test_default_order_pk_desc(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DESC" in sql_text.upper()

    async def test_limit_on_m2m_uses_lateral(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("roles",), limit=1)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "LATERAL" in sql_text.upper()
        assert "1" in sql_text

    async def test_limit_on_self_ref(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("children",), limit=1)
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")

        assert len(root.children) <= 1
