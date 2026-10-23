
import unittest


def is_valid_order(order):
    required_fields = [
        "event_id",
        "order_id",
        "category",
        "quantity",
        "unit_price",
        "event_time",
    ]

    if not isinstance(order, dict):
        return False

    if any(order.get(field) is None for field in required_fields):
        return False

    if not isinstance(order["quantity"], int) or isinstance(order["quantity"], bool):
        return False

    if order["quantity"] <= 0:
        return False

    if not isinstance(order["unit_price"], (int, float)) or isinstance(order["unit_price"], bool):
        return False

    if order["unit_price"] <= 0:
        return False

    if not all(isinstance(order[field], str) and order[field].strip() for field in [
        "event_id", "order_id", "category", "event_time"
    ]):
        return False

    return True


class TestOrderValidation(unittest.TestCase):

    def setUp(self):
        self.valid_order = {
            "event_id": "event-001",
            "order_id": "order-001",
            "category": "Electronics",
            "quantity": 2,
            "unit_price": 99.99,
            "event_time": "2026-10-23T10:00:00+00:00",
        }

    def test_valid_order(self):
        self.assertTrue(is_valid_order(self.valid_order))

    def test_missing_order_id(self):
        order = self.valid_order.copy()
        order["order_id"] = None
        self.assertFalse(is_valid_order(order))

    def test_zero_quantity(self):
        order = self.valid_order.copy()
        order["quantity"] = 0
        self.assertFalse(is_valid_order(order))

    def test_negative_price(self):
        order = self.valid_order.copy()
        order["unit_price"] = -10
        self.assertFalse(is_valid_order(order))

    def test_missing_category(self):
        order = self.valid_order.copy()
        order["category"] = None
        self.assertFalse(is_valid_order(order))

    def test_invalid_record_type(self):
        self.assertFalse(is_valid_order(None))


if __name__ == "__main__":
    unittest.main()
