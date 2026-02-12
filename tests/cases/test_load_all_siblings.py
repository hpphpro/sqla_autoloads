from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Post, User

pytestmark = pytest.mark.anyio
_limit_params = pytest.mark.parametrize("limit", [
    pytest.param(None, id="no_limit"),
    pytest.param(50, marks=pytest.mark.lateral, id="limit_50"),
])


class TestLoadAllSiblings:
    @_limit_params
    async def test_user_posts_and_deep_reactions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts", "posts.comments.reactions"),
            limit=limit,
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_user_all_direct_plus_deep(
        self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts", "roles", "profile", "posts.comments.reactions"),
            limit=limit,
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        assert len(alice.roles) == 2
        assert alice.profile is not None

        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_user_multiple_deep_paths(
        self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None
    ) -> None:
        query = sqla_select(
            model=User,
            loads=(
                "posts.comments.reactions",
                "posts.tags",
                "posts.attachments",
            ),
            limit=limit,
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        post1 = next(p for p in alice.posts if p.id == 1)
        assert len(post1.tags) == 2
        assert len(post1.attachments) == 2

        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_post_comments_tags_attachments_deep(
        self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None
    ) -> None:
        query = sqla_select(
            model=Post,
            loads=("comments", "tags", "attachments", "comments.reactions"),
            limit=limit,
        )
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        post1 = next(p for p in posts if p.id == 1)

        assert len(post1.comments) == 2
        assert len(post1.tags) == 2
        assert len(post1.attachments) == 2

        all_reactions = []
        for comment in post1.comments:
            all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_user_messages_all_fks_plus_posts(
        self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("sent_messages", "received_messages", "posts", "roles"),
            limit=limit,
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2
        assert len(alice.received_messages) == 1
        assert len(alice.posts) == 3
        assert len(alice.roles) == 2

    @_limit_params
    async def test_user_everything(
        self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None
    ) -> None:
        query = sqla_select(
            model=User,
            loads=(
                "posts",
                "roles",
                "sent_messages",
                "received_messages",
                "owned_messages",
                "profile",
                "posts.comments.reactions",
                "posts.tags",
                "posts.attachments",
                "posts.comments.attachments",
            ),
            limit=limit,
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        assert len(alice.roles) == 2
        assert len(alice.sent_messages) == 2
        assert len(alice.received_messages) == 1
        assert len(alice.owned_messages) == 1
        assert alice.profile is not None

        post1 = next(p for p in alice.posts if p.id == 1)
        assert len(post1.tags) == 2
        assert len(post1.attachments) == 2
        assert len(post1.comments) == 2

        all_reactions = []
        all_comment_attachments = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
                all_comment_attachments.extend(comment.attachments)
        assert len(all_reactions) == 2
        assert len(all_comment_attachments) == 1
