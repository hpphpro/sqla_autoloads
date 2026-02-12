from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import (
    Category,
    Comment,
    Message,
    Post,
    PostTag,
    Profile,
    Reaction,
    Role,
    Tag,
    User,
    Base,
)

pytestmark = pytest.mark.anyio

_limit_params = pytest.mark.parametrize("limit", [
    pytest.param(None, id="no_limit"),
    pytest.param(50, marks=pytest.mark.lateral, id="limit_50"),
])


class TestLoadAllDirect:
    @_limit_params
    async def test_user_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=User,
            loads=("posts", "roles", "sent_messages", "received_messages", "owned_messages", "profile"),
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
        assert alice.profile.bio == "Alice bio"

        charlie = next(u for u in users if u.name == "charlie")
        assert len(charlie.posts) == 0
        assert charlie.profile is None

    @_limit_params
    async def test_post_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=Post,
            loads=("author", "comments", "tags", "attachments"),
            limit=limit,
        )
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        post1 = next(p for p in posts if p.id == 1)

        assert post1.author is not None
        assert post1.author.name == "alice"
        assert len(post1.comments) == 2
        assert len(post1.tags) == 2
        tag_names = {t.name for t in post1.tags}
        assert tag_names == {"python", "sqlalchemy"}
        assert len(post1.attachments) == 2

        post3 = next(p for p in posts if p.id == 3)
        assert len(post3.comments) == 0
        assert len(post3.tags) == 0
        assert len(post3.attachments) == 0

    @_limit_params
    async def test_comment_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(
            model=Comment,
            loads=("post", "reactions", "attachments"),
            limit=limit,
        )
        result = await session.execute(query)
        comments = result.unique().scalars().all()
        c1 = next(c for c in comments if c.id == 1)

        assert c1.post is not None
        assert c1.post.title == "Alice Post 1"
        assert len(c1.reactions) == 2
        assert len(c1.attachments) == 1

        c2 = next(c for c in comments if c.id == 2)
        assert len(c2.reactions) == 0
        assert len(c2.attachments) == 0

    async def test_message_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("from_user", "to_user", "owner"))
        result = await session.execute(query)
        messages = result.unique().scalars().all()
        msg1 = next(m for m in messages if m.id == 1)

        assert msg1.from_user.name == "alice"
        assert msg1.to_user.name == "bob"
        assert msg1.owner.name == "alice"

    @_limit_params
    async def test_role_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(model=Role, loads=("users",), limit=limit)
        result = await session.execute(query)
        roles = result.unique().scalars().all()
        admin = next(r for r in roles if r.name == "admin")

        assert len(admin.users) == 1
        assert admin.users[0].name == "alice"

        editor = next(r for r in roles if r.name == "editor")
        assert len(editor.users) == 2

    @_limit_params
    async def test_category_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(model=Category, loads=("parent", "children"), limit=limit)
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")
        assert root.parent is None
        assert len(root.children) == 2

        child1 = next(c for c in categories if c.name == "child_1")
        assert child1.parent is not None
        assert child1.parent.name == "root"
        assert len(child1.children) == 1

    @_limit_params
    async def test_tag_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int | None) -> None:
        query = sqla_select(model=Tag, loads=("posts",), limit=limit)
        result = await session.execute(query)
        tags = result.unique().scalars().all()
        python_tag = next(t for t in tags if t.name == "python")

        assert len(python_tag.posts) == 2

        testing_tag = next(t for t in tags if t.name == "testing")
        assert len(testing_tag.posts) == 1

    async def test_profile_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Profile, loads=("user",))
        result = await session.execute(query)
        profiles = result.unique().scalars().all()
        p1 = next(p for p in profiles if p.id == 1)

        assert p1.user is not None
        assert p1.user.name == "alice"

    async def test_reaction_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Reaction, loads=("comment",))
        result = await session.execute(query)
        reactions = result.unique().scalars().all()
        r1 = next(r for r in reactions if r.id == 1)

        assert r1.comment is not None
        assert r1.comment.text == "Great post!"

    async def test_post_tag_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=PostTag, loads=("post", "tag"))
        result = await session.execute(query)
        post_tags = result.unique().scalars().all()

        pt = next(pt for pt in post_tags if pt.post_id == 1 and pt.tag_id == 1)
        assert pt.post is not None
        assert pt.post.title == "Alice Post 1"
        assert pt.tag is not None
        assert pt.tag.name == "python"
