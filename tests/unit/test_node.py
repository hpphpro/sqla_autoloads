from __future__ import annotations

import pytest
from sqlalchemy import orm

from sqla_autoloads.node import Node, get_node, init_node

from ..models import Base, Post, User


class TestNodeSingleton:
    def test_uninitialized_raises_runtime_error(self, reset_node_singleton: None) -> None:
        Node._Node__instance = None  # type: ignore[attr-defined]
        with pytest.raises(RuntimeError, match="not initialized"):
            Node()

    def test_singleton_returns_same_instance(self) -> None:
        n1 = Node()
        n2 = Node()
        assert n1 is n2

    def test_init_node_initializes(self, reset_node_singleton: None) -> None:
        Node.reset()
        mapping = get_node(Base)
        init_node(mapping)
        node = Node()

        assert node.node is mapping


class TestNodeAccess:
    def test_get_returns_relationships(self) -> None:
        node = Node()
        rels = node.get(User)
        assert len(rels) > 0
        keys = {r.key for r in rels}

        assert "posts" in keys

    def test_get_unknown_model_returns_empty(self) -> None:
        node = Node()

        class Dummy(orm.DeclarativeBase):
            pass

        result = node.get(Dummy)
        assert result == ()

    def test_getitem(self) -> None:
        node = Node()
        rels = node[User]
        assert len(rels) > 0

    def test_getitem_missing_raises_keyerror(self) -> None:
        node = Node()

        class Dummy(orm.DeclarativeBase):
            pass

        with pytest.raises(KeyError):
            _ = node[Dummy]


class TestGetNode:
    def test_returns_frozendict(self) -> None:
        mapping = get_node(Base)
        assert User in mapping
        assert Post in mapping

    def test_assertion_on_non_base(self) -> None:
        with pytest.raises(AssertionError, match="subclass of orm.DeclarativeBase"):
            get_node(User)  # type: ignore[arg-type]


class TestSetNode:
    def test_set_node_updates_mapping(self) -> None:
        node = Node()
        original = node.node
        new_mapping = get_node(Base)
        node.set_node(new_mapping)
        assert node.node is new_mapping
        node.set_node(original)
