"""Tests for the SSE stream router."""

from app.market.cache import PriceCache
from app.market.stream import create_stream_router


class TestCreateStreamRouter:
    """Each router must be independent and bound to its own cache."""

    def test_each_call_returns_a_new_router(self):
        """Two calls must not share (and pile routes onto) one router."""
        first = create_stream_router(PriceCache())
        second = create_stream_router(PriceCache())
        assert first is not second

    def test_router_has_single_prices_route(self):
        """A router exposes exactly one /api/stream/prices route."""
        create_stream_router(PriceCache())
        router = create_stream_router(PriceCache())
        assert [route.path for route in router.routes] == ["/api/stream/prices"]
