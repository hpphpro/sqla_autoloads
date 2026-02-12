from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, User

pytestmark = pytest.mark.anyio

class TestSiblingLoading:
    async def test_two_siblings(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None  :
        query = sqla_select(model=User, loads=("posts", "roles"))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3
        role_names = {r.name for r in alice.roles}
        assert role_names == {"admin", "editor"}

    async def test_deep_plus_non_overlapping_sibling(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Deep reactions + sibling roles: no shared intermediates, should work."""
        query = sqla_select(model=User, loads=("reactions", "roles"))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        role_names = {r.name for r in alice.roles}
        assert role_names == {"admin", "editor"}

        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)

        assert len(all_reactions) == 2

    async def test_deep_reactions_after_sibling_posts(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Load order: pop() gives 'posts' first, then 'reactions'.
        'posts' adds Post to excludes. 'reactions' chain [posts,comments,reactions]
        skips posts (already excluded), breaking the chained eager load.
        """
        query = sqla_select(model=User, loads=("reactions", "posts"))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3

        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)

        assert len(all_reactions) == 2
