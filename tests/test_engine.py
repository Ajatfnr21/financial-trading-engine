"""Tests for Financial Trading Engine"""

import pytest
from fastapi.testclient import TestClient
from decimal import Decimal

from engine.main import fastapi_app, engine, OrderRequest, OrderSide, OrderType, TimeInForce

client = TestClient(fastapi_app)


class TestHealth:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "active_orders" in data

    def test_info(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "Financial Trading Engine" in response.json()["name"]


class TestOrders:
    def test_submit_market_order(self):
        # First add some liquidity
        client.post("/orders", json={
            "symbol": "BTCUSD",
            "side": "sell",
            "order_type": "limit",
            "quantity": "1.0",
            "price": "50000.00",
            "user_id": "market_maker"
        })
        
        # Submit market buy
        response = client.post("/orders", json={
            "symbol": "BTCUSD",
            "side": "buy",
            "order_type": "market",
            "quantity": "0.5",
            "user_id": "buyer"
        })
        assert response.status_code == 200
        data = response.json()
        assert data["side"] == "buy"
        assert "order_id" in data

    def test_submit_limit_order(self):
        response = client.post("/orders", json={
            "symbol": "ETHUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": "10.0",
            "price": "3000.00",
            "user_id": "buyer"
        })
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["open", "filled"]
        assert data["price"] == 3000.00

    def test_get_order(self):
        # Create order
        create_response = client.post("/orders", json={
            "symbol": "TESTUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": "1.0",
            "price": "100.00"
        })
        order_id = create_response.json()["order_id"]
        
        # Get it
        response = client.get(f"/orders/{order_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["order_id"] == order_id

    def test_cancel_order(self):
        # Create order
        create_response = client.post("/orders", json={
            "symbol": "TESTUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": "1.0",
            "price": "100.00",
            "user_id": "testuser"
        })
        order_id = create_response.json()["order_id"]
        
        # Cancel it
        response = client.delete(f"/orders/{order_id}?user_id=testuser")
        assert response.status_code == 200
        assert response.json()["status"] == "cancelled"


class TestOrderBook:
    def test_get_order_book(self):
        # Add some orders
        client.post("/orders", json={
            "symbol": "ORDERBOOKTEST",
            "side": "buy",
            "order_type": "limit",
            "quantity": "1.0",
            "price": "99.00"
        })
        client.post("/orders", json={
            "symbol": "ORDERBOOKTEST",
            "side": "sell",
            "order_type": "limit",
            "quantity": "1.0",
            "price": "101.00"
        })
        
        response = client.get("/orderbook/ORDERBOOKTEST")
        assert response.status_code == 200
        data = response.json()
        assert "bids" in data
        assert "asks" in data


class TestMatchingEngine:
    def test_market_order_matching(self):
        # Create sell limit
        engine.submit_order(OrderRequest(
            symbol="MATCHTEST",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("100.00"),
            user_id="seller"
        ))
        
        # Create buy market
        order = engine.submit_order(OrderRequest(
            symbol="MATCHTEST",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.5"),
            user_id="buyer"
        ))
        
        assert order.filled_quantity == Decimal("0.5")
        assert order.status.value in ["filled", "partially_filled"]

    def test_position_tracking(self):
        # Create buy order
        engine.submit_order(OrderRequest(
            symbol="POSTEST",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("100.00"),
            user_id="position_user"
        ))
        
        position = engine.get_position("position_user", "POSTEST")
        assert position is not None
        assert position.quantity == Decimal("1.0")
