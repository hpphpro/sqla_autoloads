from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Post, User

pytestmark = pytest.mark.anyio

class TestDeepLoading:
    async def test_two_hop_user_to_comments(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("comments",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        all_comments = []
        for post in alice.posts:
            all_comments.extend(post.comments)

        assert len(all_comments) == 2

    async def test_three_hop_user_to_reactions(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("reactions",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    async def test_intermediate_models_loaded(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("comments",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) > 0

        post1 = next(p for p in alice.posts if p.id == 1)

        assert len(post1.comments) == 2

    async def test_deep_load_from_post(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Post, loads=("reactions",))
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        post1 = next(p for p in posts if p.id == 1)
        all_reactions = []
        for comment in post1.comments:
            all_reactions.extend(comment.reactions)

        assert len(all_reactions) == 2
