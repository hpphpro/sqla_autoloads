from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import add_conditions, sqla_select

from ..models import Attachment, Base, Category, Comment, Post, Role, Tag, User
from sqla_autoloads.core import _extract_limit
import sqlalchemy as sa
from typing import Any

pytestmark = pytest.mark.anyio


def _compile(query: sa.Select[Any]) -> str:
    return str(query.compile(compile_kwargs={"literal_binds": True}))


class TestZipOptimization:
    """Tests for the auto-ZIP optimization that eliminates row multiplication
    when 2+ sibling LATERAL subqueries are present."""

    @pytest.mark.lateral
    async def test_two_siblings_zipped(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """User(posts + roles) — both should load correctly with ZIP."""
        query = sqla_select(model=User, loads=("posts", "roles"))
        sql = _compile(query)

        # ZIP should be active: generate_series present, no ON true for siblings
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3
        assert {r.name for r in alice.roles} == {"admin", "editor"}

        bob = next(u for u in users if u.name == "bob")
        assert len(bob.posts) == 1
        assert {r.name for r in bob.roles} == {"editor", "viewer"}

        charlie = next(u for u in users if u.name == "charlie")
        assert len(charlie.posts) == 0
        assert len(charlie.roles) == 0

    @pytest.mark.lateral
    async def test_three_siblings_zipped(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """User(posts + roles + sent_messages) — 3 siblings."""
        query = sqla_select(
            model=User,
            loads=("posts", "roles", "sent_messages"),
            check_tables=True,
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3
        assert {r.name for r in alice.roles} == {"admin", "editor"}
        assert len(alice.sent_messages) == 2

    @pytest.mark.lateral
    async def test_m2m_self_contained(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """M2M (roles) in ZIP mode uses self-contained LATERAL (secondary
        join inside the subquery, no outer join to user_roles)."""
        query = sqla_select(model=User, loads=("posts", "roles"))
        sql = _compile(query)

        assert "_sqla_rn_cte" in sql.lower()
        # The roles LATERAL should contain the secondary join internally
        assert "user_roles" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert {r.name for r in alice.roles} == {"admin", "editor"}

    @pytest.mark.lateral
    async def test_chained_plus_sibling(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """posts.comments (chained) + roles (sibling) — ZIP for siblings,
        chained hops use ON TRUE."""
        query = sqla_select(model=User, loads=("posts.comments", "roles"))
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3
        assert {r.name for r in alice.roles} == {"admin", "editor"}

        post1 = next(p for p in alice.posts if p.title == "Alice Post 1")
        assert len(post1.comments) == 2

    @pytest.mark.lateral
    async def test_single_lateral_no_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """1 relationship → no ZIP, no CTE series."""
        query = sqla_select(model=User, loads=("posts",))
        sql = _compile(query)

        assert "_sqla_rn_cte" not in sql.lower()
        assert "lateral" in sql.lower()

    @pytest.mark.lateral
    async def test_conditions_with_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Conditions should work inside ZIP LATERALs."""
        query = sqla_select(
            model=User,
            loads=("posts", "roles"),
            conditions={
                "posts": add_conditions(Post.title == "Alice Post 1"),
                "roles": add_conditions(Role.level >= 5),
            },
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 1
        assert alice.posts[0].title == "Alice Post 1"
        assert {r.name for r in alice.roles} == {"admin", "editor"}

    async def test_no_limit_no_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """limit=None → no LATERAL, no ZIP."""
        query = sqla_select(model=User, loads=("posts", "roles"), limit=None)
        sql = _compile(query)

        assert "_sqla_rn_cte" not in sql.lower()
        assert "lateral" not in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3
        assert {r.name for r in alice.roles} == {"admin", "editor"}

    @pytest.mark.lateral
    async def test_self_ref_excluded(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Category parent + children → self-referential, excluded from ZIP."""
        query = sqla_select(
            model=Category, loads=("parent", "children"), self_key="parent_id"
        )
        sql = _compile(query)

        # Self-ref uses its own mechanism (alias + selectinload fallback)
        assert "_sqla_rn_cte" not in sql.lower()

    @pytest.mark.lateral
    async def test_m2m_o2m_same_assoc_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Post(tags + post_tags) with ZIP — O2M loads association table first,
        M2M reuses its lateral for secondaryjoin."""
        query = sqla_select(
            model=Post, loads=("tags", "post_tags"), check_tables=True
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        posts = result.unique().scalars().all()

        post1 = next(p for p in posts if p.title == "Alice Post 1")
        assert len(post1.tags) == 2
        assert len(post1.post_tags) == 2
        tag_names = {t.name for t in post1.tags}
        assert tag_names == {"python", "sqlalchemy"}

    @pytest.mark.lateral
    async def test_zip_uses_recursive_cte(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """ZIP mode should use a recursive CTE, not generate_series."""
        query = sqla_select(model=User, loads=("posts", "roles"))
        sql = _compile(query)

        assert "with recursive" in sql.lower()
        assert "_sqla_rn_cte" in sql.lower()
        assert "generate_series" not in sql.lower()

    @pytest.mark.lateral
    async def test_zip_compatible_with_external_cte(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """External CTE chained after sqla_select should not conflict with ZIP CTE."""
        pk = User.id
        user_cte = sa.select(pk).where(User.active.is_(True)).cte(name="active_users")
        query = sqla_select(model=User, loads=("posts", "roles")).join(
            user_cte, pk == user_cte.c.id
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()
        assert "active_users" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()
        # Only active users (alice + bob), charlie excluded
        assert len(users) == 2
        names = {u.name for u in users}
        assert names == {"alice", "bob"}

    @pytest.mark.lateral
    async def test_zip_condition_limit_override(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Condition setting a higher limit should work — CTE adapts."""
        query = sqla_select(
            model=User,
            loads=("posts", "roles"),
            limit=50,
            conditions={
                "posts": lambda q: q.limit(None).limit(100),
            },
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()
        # CTE should go up to 100, not 50
        assert "_rn < 100" in sql or "_rn < 100" in sql.replace(" ", "")

        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3  # all alice's posts (< both 50 and 100)
        assert {r.name for r in alice.roles} == {"admin", "editor"}

    @pytest.mark.lateral
    async def test_zip_condition_order_by_override(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Condition changing ORDER BY should not lose rows."""
        query = sqla_select(
            model=User,
            loads=("posts", "roles"),
            conditions={
                "posts": lambda q: q.order_by(None).order_by(Post.title.asc()),
            },
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        assert {r.name for r in alice.roles} == {"admin", "editor"}


class TestExtractLimit:

    def test_explicit_limit(self) -> None:
        q = sa.select(sa.literal(1)).limit(100)

        assert _extract_limit(q, 50) == 100

    def test_no_limit_returns_default(self) -> None:
        q = sa.select(sa.literal(1))

        assert _extract_limit(q, 50) == 50

    def test_limit_none_returns_default(self) -> None:
        q = sa.select(sa.literal(1)).limit(None)

        assert _extract_limit(q, 50) == 50

    def test_limit_override(self) -> None:
        q = sa.select(sa.literal(1)).limit(50).limit(None).limit(200)

        assert _extract_limit(q, 50) == 200

    def test_limit_override2(self) -> None:
        q = sa.select(sa.literal(1)).limit(200)

        assert _extract_limit(q, 50) == 200


class TestDeepZip:
    """Tests for deep-level ZIP optimization across all depth levels."""

    @pytest.mark.lateral
    async def test_deep_two_siblings(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # posts.comments + posts.tags — depth 1 siblings should ZIP."""
        query = sqla_select(model=User, loads=("posts.comments", "posts.tags"))
        sql = _compile(query)

        assert "_sqla_rn_cte" in sql.lower()
        # Should have 2 RN series (depth 0 for posts is single, depth 1 for comments+tags)
        assert "_sqla_rn" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3

        post1 = next(p for p in alice.posts if p.title == "Alice Post 1")
        assert len(post1.comments) == 2
        assert len(post1.tags) == 2
        assert {t.name for t in post1.tags} == {"python", "sqlalchemy"}

    @pytest.mark.lateral
    async def test_deep_three_siblings(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # posts.comments + posts.tags + posts.attachments — 3 siblings at depth 1."""
        query = sqla_select(
            model=User,
            loads=("posts.comments", "posts.tags", "posts.attachments"),
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3

        post1 = next(p for p in alice.posts if p.title == "Alice Post 1")
        assert len(post1.comments) == 2
        assert len(post1.tags) == 2
        assert len(post1.attachments) == 2

    @pytest.mark.lateral
    async def test_deep_mixed_depths(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # posts.comments + posts.tags + roles — ZIP at depth 0 AND depth 1."""
        query = sqla_select(
            model=User,
            loads=("posts.comments", "posts.tags", "roles"),
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        # Should have 2 RN subqueries: _sqla_rn (depth 0) and _sqla_rn_1 (depth 1)
        assert "_sqla_rn_1" in sql.lower() or "_sqla_rn_1" in sql

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) == 3
        assert {r.name for r in alice.roles} == {"admin", "editor"}

        post1 = next(p for p in alice.posts if p.title == "Alice Post 1")
        assert len(post1.comments) == 2
        assert len(post1.tags) == 2

    @pytest.mark.lateral
    async def test_deep_depth_2(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # posts.comments.reactions + posts.comments.attachments — depth 2 siblings."""
        query = sqla_select(
            model=User,
            loads=("posts.comments.reactions", "posts.comments.attachments"),
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        post1 = next(p for p in alice.posts if p.title == "Alice Post 1")
        comment1 = next(c for c in post1.comments if c.text == "Great post!")
        assert len(comment1.reactions) == 2
        assert len(comment1.attachments) == 1

    @pytest.mark.lateral
    async def test_deep_single_no_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # Single deep hop — no ZIP needed."""
        query = sqla_select(model=User, loads=("posts.comments",))
        sql = _compile(query)

        # No ZIP: only 1 uselist at each depth
        assert "_sqla_rn_cte" not in sql.lower()
        assert "lateral" in sql.lower()

    @pytest.mark.lateral
    async def test_deep_with_conditions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # Conditions work inside deep ZIP LATERALs.
        query = sqla_select(
            model=User,
            loads=("posts.comments", "posts.tags"),
            conditions={
                "comments": add_conditions(Comment.text == "Great post!"),
            },
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" in sql.lower()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        alice = next(u for u in users if u.name == "alice")
        post1 = next(p for p in alice.posts if p.title == "Alice Post 1")
        assert len(post1.comments) == 1
        assert post1.comments[0].text == "Great post!"
        assert len(post1.tags) == 2


class TestOptimizationOff:
    """optimization=False disables ZIP — plain LATERAL ON TRUE joins."""

    @pytest.mark.lateral
    async def test_two_siblings_no_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Two O2M siblings with optimization=False → no _sqla_rn_cte in SQL."""
        query = sqla_select(model=User, loads=("posts", "roles"), optimization=False)
        sql = _compile(query)
        assert "_sqla_rn_cte" not in sql.lower()
        assert "lateral" in sql.lower()
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        assert len(alice.posts) > 0
        assert len(alice.roles) > 0

    @pytest.mark.lateral
    async def test_m2m_siblings_no_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """M2M + O2M siblings with optimization=False."""
        query = sqla_select(
            model=Post, loads=("tags", "comments"), optimization=False, check_tables=True
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" not in sql.lower()
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        post1 = next(p for p in posts if p.title == "Alice Post 1")
        assert len(post1.tags) > 0
        assert len(post1.comments) > 0

    @pytest.mark.lateral
    async def test_deep_siblings_no_zip(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Deep-level siblings with optimization=False → no CTE at any depth."""
        query = sqla_select(
            model=User, loads=("posts.comments", "posts.tags"), optimization=False
        )
        sql = _compile(query)
        assert "_sqla_rn_cte" not in sql.lower()
        result = await session.execute(query)
        users = result.unique().scalars().all()
        assert len(users) > 0

    @pytest.mark.lateral
    async def test_optimization_off_same_results(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """optimization=True and False produce identical object graphs."""
        q_on = sqla_select(model=User, loads=("posts", "roles"), optimization=True)
        q_off = sqla_select(model=User, loads=("posts", "roles"), optimization=False)
        r_on = (await session.execute(q_on)).unique().scalars().all()
        r_off = (await session.execute(q_off)).unique().scalars().all()
        for u_on, u_off in zip(
            sorted(r_on, key=lambda u: u.id),
            sorted(r_off, key=lambda u: u.id),
        ):
            assert {p.id for p in u_on.posts} == {p.id for p in u_off.posts}
            assert {r.id for r in u_on.roles} == {r.id for r in u_off.roles}
