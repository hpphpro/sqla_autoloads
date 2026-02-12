from __future__ import annotations


from sqla_autoloads.core import _bfs_search
from sqla_autoloads.node import Node, get_node, init_node

from ..models import Base, Category, Comment, User


def _get_node() -> Node:
    # Ensure Node is initialized and return it.
    try:
        return Node()
    except RuntimeError:
        init_node(get_node(Base))
        return Node()


class TestBfsSearch:
    def test_direct_relationship(self) -> None:
        node = _get_node()
        path = _bfs_search(User, "posts", node)

        assert len(path) == 1
        assert path[0].key == "posts"

    def test_two_hop(self) -> None:
        node = _get_node()
        path = _bfs_search(User, "comments", node)

        assert len(path) == 2
        assert path[0].key == "posts"
        assert path[1].key == "comments"

    def test_three_hop(self) -> None:
        node = _get_node()
        path = _bfs_search(User, "reactions", node)

        assert len(path) == 3
        assert path[0].key == "posts"
        assert path[1].key == "comments"
        assert path[2].key == "reactions"

    def test_m2m_relationship(self) -> None:
        node = _get_node()
        path = _bfs_search(User, "roles", node)

        assert len(path) == 1
        assert path[0].key == "roles"

    def test_self_referential_children(self) -> None:
        node = _get_node()
        path = _bfs_search(Category, "children", node)

        assert len(path) == 1
        assert path[0].key == "children"

    def test_self_referential_parent(self) -> None:
        node = _get_node()
        path = _bfs_search(Category, "parent", node)

        assert len(path) == 1
        assert path[0].key == "parent"

    def test_nonexistent_returns_empty(self) -> None:
        node = _get_node()
        path = _bfs_search(User, "nonexistent_rel", node)

        assert path == ()

    def test_reverse_path(self) -> None:
        node = _get_node()
        path = _bfs_search(Comment, "author", node)

        assert len(path) == 2
        assert path[0].key == "post"
        assert path[1].key == "author"

    def test_multi_fk_relationship(self) -> None:
        node = _get_node()
        path = _bfs_search(User, "sent_messages", node)

        assert len(path) == 1
        assert path[0].key == "sent_messages"
