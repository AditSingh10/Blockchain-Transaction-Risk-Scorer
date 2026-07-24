from __future__ import annotations

from typing import Any


def backward_compatibility_errors(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> list[str]:
    """Detect breaking structural changes made without a schema-version bump."""

    errors: list[str] = []
    baseline_version = _schema_version(baseline)
    candidate_version = _schema_version(candidate)
    if baseline_version != candidate_version:
        return errors
    _compare_object("", baseline, candidate, errors)
    baseline_defs = baseline.get("$defs", {})
    candidate_defs = candidate.get("$defs", {})
    for name, baseline_definition in baseline_defs.items():
        candidate_definition = candidate_defs.get(name)
        if candidate_definition is None:
            errors.append(f"$defs.{name} was removed")
            continue
        _compare_object(f"$defs.{name}", baseline_definition, candidate_definition, errors)
    return errors


def _schema_version(schema: dict[str, Any]) -> str | None:
    version = schema.get("properties", {}).get("schema_version", {})
    const = version.get("const")
    if const is not None:
        return str(const)
    enum = version.get("enum", [])
    return str(enum[0]) if len(enum) == 1 else None


def _type_signature(schema: dict[str, Any]) -> Any:
    if "$ref" in schema:
        return ("ref", schema["$ref"])
    if "type" in schema:
        return schema["type"]
    if "anyOf" in schema:
        return tuple(sorted(str(_type_signature(value)) for value in schema["anyOf"]))
    return None


def _compare_object(
    path: str,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    errors: list[str],
) -> None:
    baseline_required = set(baseline.get("required", []))
    candidate_required = set(candidate.get("required", []))
    for field in sorted(baseline_required - candidate_required):
        errors.append(f"{path or '$'}.{field} is no longer required")
    baseline_properties = baseline.get("properties", {})
    candidate_properties = candidate.get("properties", {})
    for field, baseline_field in baseline_properties.items():
        field_path = f"{path}.{field}" if path else field
        candidate_field = candidate_properties.get(field)
        if candidate_field is None:
            errors.append(f"{field_path} was removed")
            continue
        if _type_signature(baseline_field) != _type_signature(candidate_field):
            errors.append(f"{field_path} changed type")
        _compare_object(field_path, baseline_field, candidate_field, errors)
