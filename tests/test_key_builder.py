"""Unit tests for the two-key identity model in key_builder."""

import pytest
from app.services.key_builder import (
    DELIMITER,
    MAX_KEY_LENGTH,
    build_entity_key,
    build_idempotent_key,
    parse_entity_key,
    parse_idempotent_key,
)


class TestEntityKey:

    def test_basic_structure(self):
        key = build_entity_key("oms", "order", "ORD-001")
        parts = key.split(DELIMITER)
        assert len(parts) == 3
        assert parts == ["oms", "order", "ORD-001"]

    def test_deterministic(self):
        assert build_entity_key("oms", "order", "ORD-001") == build_entity_key("oms", "order", "ORD-001")

    def test_different_systems_differ(self):
        assert build_entity_key("oms", "order", "ORD-001") != build_entity_key("wms", "order", "ORD-001")

    def test_business_key_with_delimiter_is_encoded(self):
        key = build_entity_key("oms", "order", f"ORD{DELIMITER}001")
        parts = key.split(DELIMITER)
        assert parts[2].startswith("b64:")

    def test_never_exceeds_max_length(self):
        key = build_entity_key("oms", "order", "X" * 1000)
        assert len(key) <= MAX_KEY_LENGTH

    def test_long_key_uses_sha256_segment(self):
        key = build_entity_key("oms", "order", "X" * 1000)
        parts = key.split(DELIMITER)
        assert parts[2].startswith("sha256:")

    def test_parse_round_trip(self):
        key = build_entity_key("oms", "order", "ORD-001")
        parsed = parse_entity_key(key)
        assert parsed == {"source_system": "oms", "entity_type": "order", "business_key": "ORD-001"}

    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_entity_key("only:::two")


class TestIdempotentKey:

    def test_is_entity_key_plus_version(self):
        entity_key = build_entity_key("oms", "order", "ORD-001")
        idempotent_key = build_idempotent_key("oms", "order", "ORD-001", 3)
        assert idempotent_key == f"{entity_key}{DELIMITER}3"

    def test_different_versions_differ(self):
        k1 = build_idempotent_key("oms", "order", "ORD-001", 1)
        k2 = build_idempotent_key("oms", "order", "ORD-001", 2)
        assert k1 != k2

    def test_deterministic(self):
        assert (
            build_idempotent_key("oms", "order", "ORD-001", 1)
            == build_idempotent_key("oms", "order", "ORD-001", 1)
        )

    def test_parse_round_trip(self):
        key = build_idempotent_key("oms", "order", "ORD-001", 5)
        parsed = parse_idempotent_key(key)
        assert parsed["source_system"] == "oms"
        assert parsed["entity_type"] == "order"
        assert parsed["version"] == "5"

    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_idempotent_key("only:::three:::parts")

