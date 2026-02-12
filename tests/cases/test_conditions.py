from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import add_conditions, sqla_select

from ..models import Base, Category, Comment, Post, Role, User

pytestmark = pytest.mark.anyio


class TestConditions:
    async def test_filter_o2m(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(
            model=User,
            loads=("posts",),
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 1
        assert alice.posts[0].title == "Alice Post 1"

    async def test_filter_m2m(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(
            model=User,
            loads=("roles",),
            conditions={"roles": add_conditions(Role.level > 3)},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        role_names = {r.name for r in alice.roles}

        assert "viewer" not in role_names

    async def test_lambda_style(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(
            model=User,
            loads=("posts",),
            conditions={"posts": lambda q: q.where(Post.title.like("%Post 1%"))},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert all("Post 1" in p.title for p in alice.posts)

    @pytest.mark.lateral
    async def test_self_referential_conditions(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(
            model=Category,
            loads=("children",),
            conditions={"children": add_conditions(Category.name == "child_1")},
        )
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")

        assert len(root.children) == 1
        assert root.children[0].name == "child_1"

    async def test_no_match_returns_empty(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(
            model=User,
            loads=("posts",),
            conditions={"posts": add_conditions(Post.title == "NONEXISTENT")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()

        for user in users:
            assert len(user.posts) == 0

    async def test_o2m_conditions_no_lateral(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts",),
            limit=None,
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        bob = next(u for u in users if u.name == "bob")

        assert len(alice.posts) == 1
        assert alice.posts[0].title == "Alice Post 1"
        assert len(bob.posts) == 0

    async def test_m2m_conditions_no_lateral(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("roles",),
            limit=None,
            conditions={"roles": add_conditions(Role.level >= 5)},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        bob = next(u for u in users if u.name == "bob")

        alice_role_names = {r.name for r in alice.roles}
        bob_role_names = {r.name for r in bob.roles}

        assert alice_role_names == {"admin", "editor"}
        assert bob_role_names == {"editor"}

    async def test_o2m_conditions_selectinload(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts",),
            limit=None,
            many_load="selectinload",
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 1
        assert alice.posts[0].title == "Alice Post 1"


    async def test_self_ref_children_no_lateral(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Category,
            loads=("children",),
            limit=None,
            conditions={"children": add_conditions(Category.name == "child_1")},
        )
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")

        assert len(root.children) == 1
        assert root.children[0].name == "child_1"

    async def test_self_ref_parent_conditions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Category,
            loads=("parent",),
            conditions={"parent": add_conditions(Category.name == "root")},
        )
        result = await session.execute(query)
        categories = result.unique().scalars().all()

        child_1 = next(c for c in categories if c.name == "child_1")
        child_2 = next(c for c in categories if c.name == "child_2")

        assert child_1.parent is not None
        assert child_1.parent.name == "root"
        assert child_2.parent is not None
        assert child_2.parent.name == "root"


    async def test_m2o_conditions_outerjoin(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Post,
            loads=("author",),
            conditions={"author": add_conditions(User.active.is_(True))},
        )
        result = await session.execute(query)
        posts = result.unique().scalars().all()

        assert len(posts) == 4
        for post in posts:
            assert post.author is not None
            assert post.author.active is True

    async def test_multiple_conditions_on_different_rels(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts", "roles"),
            conditions={
                "posts": add_conditions(Post.title == "Alice Post 1"),
                "roles": add_conditions(Role.level >= 5),
            },
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 1
        assert alice.posts[0].title == "Alice Post 1"
        assert {r.name for r in alice.roles} == {"admin", "editor"}

    async def test_conditions_with_multiple_predicates(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts",),
            conditions={"posts": add_conditions(Post.title.like("Alice%"), Post.id <= 2)},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 2
        assert {p.title for p in alice.posts} == {"Alice Post 1", "Alice Post 2"}

    async def test_conditions_on_deep_dotted_path(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts.comments",),
            conditions={"comments": add_conditions(Comment.text == "Great post!")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        post_with_comments = next(
            (p for p in alice.posts if len(p.comments) > 0), None
        )
        assert post_with_comments is not None
        assert len(post_with_comments.comments) == 1
        assert post_with_comments.comments[0].text == "Great post!"

    async def test_conditions_on_unloaded_rel_ignored(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Conditions on a relationship not in loads should be safely ignored."""
        query = sqla_select(
            model=User,
            loads=("roles",),
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) == 3

    async def test_noop_condition(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Condition that doesn't add a WHERE clause (returns query unchanged)."""
        query = sqla_select(
            model=User,
            loads=("posts",),
            limit=None,
            conditions={"posts": lambda q: q},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3

    async def test_conditions_on_multiple_levels_of_dotted_path(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Conditions on both posts and comments in a dotted path."""
        query = sqla_select(
            model=User,
            loads=("posts.comments",),
            conditions={
                "posts": add_conditions(Post.title == "Alice Post 1"),
                "comments": add_conditions(Comment.text == "Great post!"),
            },
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) >= 1
        post1 = next((p for p in alice.posts if p.title == "Alice Post 1"), None)
        assert post1 is not None
        for comment in post1.comments:
            assert comment.text == "Great post!"
