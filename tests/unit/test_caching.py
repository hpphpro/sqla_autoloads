from __future__ import annotations


from sqla_autoloads.core import _bfs_search, _select_with_relationships, _LoadParams
from sqla_autoloads.node import Node, get_node, init_node
from sqla_autoloads.tools import get_table_name, _get_table_name

from ..models import Base, User


def _get_node() -> Node:
    # Ensure Node is initialized and return it.
    try:
        return Node()
    except RuntimeError:
        init_node(get_node(Base))
        return Node()


class TestLruCaching:
    def test_same_params_return_same_object(self) -> None:
        node = _get_node()
        r1 = _bfs_search(User, "posts", node)
        r2 = _bfs_search(User, "posts", node)

        assert r1 is r2

    def test_different_params_return_different(self) -> None:
        node = _get_node()
        r1 = _bfs_search(User, "posts", node)
        r2 = _bfs_search(User, "roles", node)

        assert r1 is not r2

    def test_bfs_cache_info_hits(self) -> None:
        _bfs_search.cache_clear()
        node = _get_node()
        _bfs_search(User, "posts", node)
        _bfs_search(User, "posts", node)
        info = _bfs_search.cache_info()

        assert info.hits >= 1

    def test_cache_clear_resets(self) -> None:
        node = _get_node()
        _bfs_search(User, "posts", node)
        _bfs_search.cache_clear()
        info = _bfs_search.cache_info()

        assert info.currsize == 0

    def test_get_table_name_cached(self) -> None:
        _get_table_name.cache_clear()
        get_table_name(User)
        get_table_name(User)
        info = _get_table_name.cache_info()

        assert info.hits >= 1

    def test_select_with_relationships_cached(self) -> None:
        _select_with_relationships.cache_clear()
        node = _get_node()
        params = _LoadParams(model=User, loads=("posts",), node=node)
        q1 = _select_with_relationships(params)
        q2 = _select_with_relationships(params)

        assert q1 is q2
