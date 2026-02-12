from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Post, User

pytestmark = pytest.mark.anyio


class TestManyToOne:
    async def test_basic_load(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Post, loads=("author",))
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        for post in posts:
            assert post.author is not None
            assert isinstance(post.author, User)

    async def test_outerjoin_in_sql(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Post, loads=("author",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))
        upper = sql_text.upper()
        assert "JOIN" in upper
        assert "users" in sql_text

    async def test_all_posts_have_author(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Post, loads=("author",))
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        alice_posts = [p for p in posts if p.author.name == "alice"]
        assert len(alice_posts) == 3
