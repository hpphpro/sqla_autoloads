from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Category

pytestmark = pytest.mark.anyio



class TestSelfReferential:
    async def test_m2o_parent(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("parent",))
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        child1 = next(c for c in categories if c.name == "child_1")

        assert child1.parent is not None
        assert child1.parent.name == "root"

    async def test_root_parent_is_none(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("parent",))
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")

        assert root.parent is None

    async def test_o2m_children(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("children",))
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")
        child_names = {c.name for c in root.children}

        assert child_names == {"child_1", "child_2"}

    async def test_leaf_no_children(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("children",))
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        grandchild = next(c for c in categories if c.name == "grandchild")

        assert len(grandchild.children) == 0

    async def test_auto_detect_self_key(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        from sqla_autoloads.core import _find_self_key

        key = _find_self_key(Category)

        assert key == "parent_id"

    async def test_alias_in_sql(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("children",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "categories_children" in sql_text

    @pytest.mark.parametrize("limit", [
        pytest.param(None, id="no_limit"),
        pytest.param(50, marks=pytest.mark.lateral, id="limit_50"),
    ])
    async def test_parent_and_children_together(self, session: AsyncSession, seed_data: dict[str, list[Base]], limit: int) -> None:
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

        grandchild = next(c for c in categories if c.name == "grandchild")

        assert grandchild.parent is not None
        assert grandchild.parent.name == "child_1"
        assert len(grandchild.children) == 0

    async def test_joinedload_for_m2o_side(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Category, loads=("parent",))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "LEFT OUTER JOIN" in sql_text

    async def test_children_and_parent_no_limit(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Category, loads=("children", "parent"), limit=None
        )
        result = await session.execute(query)
        categories = result.unique().scalars().all()

        root = next(c for c in categories if c.name == "root")
        assert root.parent is None
        assert len(root.children) == 2

        child1 = next(c for c in categories if c.name == "child_1")
        assert child1.parent is not None
        assert child1.parent.name == "root"

    async def test_self_ref_with_order_by(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Category, loads=("children",), order_by=("name",)
        )
        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")

        assert len(root.children) == 2
