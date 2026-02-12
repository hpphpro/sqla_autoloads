from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Post, Role, Tag, User

pytestmark = pytest.mark.anyio

_limit_params = pytest.mark.parametrize("limit", [
    pytest.param(None, id="no_limit"),
    pytest.param(50, marks=pytest.mark.lateral, id="limit_50"),
])


class TestLoadAllDeep:
    @_limit_params
    async def test_user_deep_reactions(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=User, loads=("posts.comments.reactions",), limit=limit
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_user_deep_post_attachments(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=User, loads=("posts.attachments",), limit=limit
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        post1 = next(p for p in alice.posts if p.id == 1)
        assert len(post1.attachments) == 2

        post2 = next(p for p in alice.posts if p.id == 2)
        assert len(post2.attachments) == 0

    @_limit_params
    async def test_user_deep_comment_attachments(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=User, loads=("posts.comments.attachments",), limit=limit
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        all_attachments = []
        for post in alice.posts:
            for comment in post.comments:
                all_attachments.extend(comment.attachments)
        assert len(all_attachments) == 1

    @_limit_params
    async def test_user_deep_post_tags(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=User, loads=("posts.tags",), limit=limit
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        post1 = next(p for p in alice.posts if p.id == 1)
        assert len(post1.tags) == 2
        tag_names = {t.name for t in post1.tags}
        assert tag_names == {"python", "sqlalchemy"}

    @_limit_params
    async def test_post_deep_comment_reactions(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=Post, loads=("comments.reactions",), limit=limit
        )
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        post1 = next(p for p in posts if p.id == 1)

        all_reactions = []
        for comment in post1.comments:
            all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_role_deep_users_posts(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=Role, loads=("users.posts",), limit=limit
        )
        result = await session.execute(query)
        roles = result.unique().scalars().all()
        admin = next(r for r in roles if r.name == "admin")

        assert len(admin.users) == 1
        assert len(admin.users[0].posts) == 3

    @_limit_params
    async def test_tag_deep_posts_author(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=Tag, loads=("posts.author",), limit=limit
        )
        result = await session.execute(query)
        tags = result.unique().scalars().all()
        python_tag = next(t for t in tags if t.name == "python")

        assert len(python_tag.posts) == 2
        for post in python_tag.posts:
            assert post.author is not None

    @_limit_params
    async def test_tag_deep_posts_comments_reactions(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=Tag, loads=("posts.comments.reactions",), limit=limit
        )
        result = await session.execute(query)
        tags = result.unique().scalars().all()
        python_tag = next(t for t in tags if t.name == "python")

        all_reactions = []
        for post in python_tag.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    @_limit_params
    async def test_user_deep_sent_to_user(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=User, loads=("sent_messages.to_user",), limit=limit
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2
        to_names = {m.to_user.name for m in alice.sent_messages}
        assert to_names == {"bob", "charlie"}
