from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Post, PostTag, Role, Tag, User

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


class TestM2MWithAssociationTable:

    @pytest.mark.lateral
    async def test_m2m_and_o2m_association_no_duplicate_joins(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Post, loads=("tags", "post_tags"), check_tables=True
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        # The association table should appear only as a LATERAL, not as
        # both a raw JOIN and a LATERAL.
        join_count = sql_text.lower().count("join")
        post_tags_refs = [
            part
            for part in sql_text.lower().split("join")
            if "post_tags" in part
        ]
        # Should have exactly 2 refs: LATERAL post_tags + LATERAL tags
        # (both referencing post_tags in some way), but no raw "JOIN post_tags ON".
        # The key check: no raw outerjoin on post_tags table itself.
        assert "JOIN post_tags ON" not in sql_text, (
            f"Raw JOIN post_tags should not appear; got:\n{sql_text}"
        )

    async def test_m2m_and_o2m_association_data_integrity(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # Both post.tags and post.post_tags should be correctly populated.
        query = sqla_select(
            model=Post, loads=("tags", "post_tags"), check_tables=True
        )
        result = await session.execute(query)
        posts = result.unique().scalars().all()

        post1 = next(p for p in posts if p.title == "Alice Post 1")
        assert len(post1.tags) == 2
        assert len(post1.post_tags) == 2
        tag_names = {t.name for t in post1.tags}
        assert tag_names == {"python", "sqlalchemy"}

        post4 = next(p for p in posts if p.title == "Bob Post 1")
        assert len(post4.tags) == 1
        assert len(post4.post_tags) == 1
        assert post4.tags[0].name == "testing"

    async def test_m2m_and_o2m_reverse_direction(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # Same test from the Tag side: Tag(posts, post_tags).
        query = sqla_select(
            model=Tag, loads=("posts", "post_tags"), check_tables=True
        )
        result = await session.execute(query)
        tags = result.unique().scalars().all()

        python_tag = next(t for t in tags if t.name == "python")
        assert len(python_tag.posts) == 2
        assert len(python_tag.post_tags) == 2
        post_titles = {p.title for p in python_tag.posts}
        assert post_titles == {"Alice Post 1", "Alice Post 2"}

    async def test_m2m_alone_unchanged(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # Regression: M2M (tags,) alone still works with raw secondary join.
        query = sqla_select(model=Post, loads=("tags",))
        result = await session.execute(query)
        posts = result.unique().scalars().all()

        post1 = next(p for p in posts if p.title == "Alice Post 1")
        tag_names = {t.name for t in post1.tags}
        assert tag_names == {"python", "sqlalchemy"}
