"""
Pydantic-settings source for pre-loaded TOML data.

Sits in the priority stack between env vars (higher) and defaults (lower).
The data dict is loaded externally by loader.py, keeping this class simple
and easy to test.
"""

from __future__ import annotations

from typing import Any

from pydantic.fields import FieldInfo
from pydantic_settings import PydanticBaseSettingsSource


class TomlSource(PydanticBaseSettingsSource):
    """Settings source backed by a plain dict loaded from a TOML file."""

    def __init__(self, settings_cls: type, data: dict[str, Any]) -> None:
        super().__init__(settings_cls)
        self._data = data

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        # Return (value, field_name, value_is_complex).
        # Returning the raw dict for nested models lets pydantic-settings
        # handle coercion into StateSettings / ReceiverSettings automatically.
        return self._data.get(field_name), field_name, False

    def __call__(self) -> dict[str, Any]:
        return self._data
