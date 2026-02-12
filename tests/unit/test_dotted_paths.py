from __future__ import annotations

import pytest

from sqla_autoloads.core import _resolve_dotted_path
from sqla_autoloads.node import Node

from ..models import  Comment, Post, Reaction, User


pytestmark = pytest.mark.usefixtures("_init_node")


class TestResolveDottedPath:
    def test_single_segment_falls_back_to_bfs(self) -> None:
        node = Node()
        result = _resolve_dotted_path(User, "posts", node)

        assert len(result) == 1
        assert result[0].key == "posts"

    def test_two_segment_path(self) -> None:
        node = Node()
        result = _resolve_dotted_path(User, "posts.comments", node)

        assert len(result) == 2
        assert result[0].key == "posts"
        assert result[1].key == "comments"

    def test_three_segment_path(self) -> None:
        node = Node()
        result = _resolve_dotted_path(User, "posts.comments.reactions", node)

        assert len(result) == 3
        assert result[0].key == "posts"
        assert result[1].key == "comments"
        assert result[2].key == "reactions"

    def test_invalid_segment_raises(self) -> None:
        node = Node()
        with pytest.raises(ValueError, match="No relationship 'nonexistent' on Post"):
            _resolve_dotted_path(User, "posts.nonexistent", node)

    def test_invalid_first_segment_raises(self) -> None:
        node = Node()
        with pytest.raises(ValueError, match="No relationship 'nonexistent' on User"):
            _resolve_dotted_path(User, "nonexistent.posts", node)

    def test_correct_mapper_classes(self) -> None:
        node = Node()
        result = _resolve_dotted_path(User, "posts.comments.reactions", node)

        assert result[0].mapper.class_ is Post
        assert result[1].mapper.class_ is Comment
        assert result[2].mapper.class_ is Reaction
