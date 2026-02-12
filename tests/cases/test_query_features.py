from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Post, Tag, User

pytestmark = pytest.mark.anyio


class TestQueryFeatures:
    async def test_distinct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), distinct=True)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

    async def test_existing_query_with_loads(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        base_query = sa.select(User).where(User.active == True)  # noqa: E712
        query = sqla_select(model=User, loads=("posts",), query=base_query)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        names = {u.name for u in users}

        assert "charlie" not in names

    async def test_query_preserves_where(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        base_query = sa.select(User).where(User.name == "alice")
        query = sqla_select(model=User, loads=("posts",), query=base_query)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) == 1
        assert users[0].name == "alice"

    async def test_check_tables(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), check_tables=True)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    def test_declarative_base_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="must not be orm.DeclarativeBase"):
            sqla_select(model=orm.DeclarativeBase, self_key="")

    async def test_empty_loads(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=())
        result = await session.execute(query)
        users = result.scalars().all()

        assert len(users) == 3

    @pytest.mark.lateral
    async def test_check_tables_with_o2m_lateral(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("posts", "sent_messages"), check_tables=True
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    async def test_check_tables_with_m2m(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("roles",), check_tables=True)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.roles) > 0

    async def test_distinct_with_m2m(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("roles",), distinct=True)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    async def test_distinct_with_deep_dotted_path(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("posts.comments.reactions",), distinct=True
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

    async def test_distinct_with_no_limit(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("posts",), limit=None, distinct=True
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0
