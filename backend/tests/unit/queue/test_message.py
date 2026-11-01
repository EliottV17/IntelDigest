"""Unit tests for the Redis Stream v1 job message contract."""

import json
from uuid import UUID

import pytest
from pydantic import ValidationError

from inteldigest.queue.message import SCHEMA_VERSION, Message

_SAMPLE_ID = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def test_message_v1_json_contract_and_round_trip() -> None:
    assert SCHEMA_VERSION == 1
    message = Message(
        schema_version=SCHEMA_VERSION,
        job_id=_SAMPLE_ID,
        url="https://example.com/article?lang=en#intro",
    )

    encoded = message.model_dump_json()
    assert encoded == (
        '{"schema_version":1,'
        '"job_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",'
        '"url":"https://example.com/article?lang=en#intro"}'
    )
    assert json.loads(encoded) == {
        "schema_version": 1,
        "job_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "url": "https://example.com/article?lang=en#intro",
    }
    assert Message.model_validate_json(encoded) == message

    with pytest.raises(ValidationError):
        message.url = "https://other.example/article"  # type: ignore[misc]


@pytest.mark.parametrize(
    "payload",
    [
        {"job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": 0, "job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": 2, "job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": -1, "job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": True, "job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": "1", "job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": 1.0, "job_id": str(_SAMPLE_ID), "url": "https://example.com/article"},
        {"schema_version": 1, "url": "https://example.com/article"},
        {
            "schema_version": 1,
            "job_id": "00000000-0000-0000-0000-000000000000",
            "url": "https://example.com/article",
        },
        {"schema_version": 1, "job_id": "not-a-uuid", "url": "https://example.com/article"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID)},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": None},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": ""},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "   "},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "not a URL"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "not-a-url"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "/relative/path"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "ftp://example.com/file"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "https://"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "https://:8080/path"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "https://example.com:bad/path"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "https://[::1/path"},
        {"schema_version": 1, "job_id": str(_SAMPLE_ID), "url": "https://example.com/a\nb"},
    ],
)
def test_message_rejects_unsupported_or_incomplete_payloads(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Message.model_validate(payload)
    with pytest.raises(ValidationError):
        Message.model_validate_json(json.dumps(payload))
