"""Human review service: rules, provenance-checked sync, correction overlay, decisions."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from api.services.artifacts import _safe_project_dir
from app import provenance as prov
from db import store

LOGGER = logging.getLogger(__name__)

RULES_FILE = "review_rules.json"
_KNOWN_RULES = ("R1", "R2", "R3", "R4")
IMPACTS = ("low", "medium", "high")
ANOMALY_PREFIX = "anomaly_report_v2_"
DQ_PREFIX = "dq_score_"
# field a reviewer may correct, per item kind, with its allowed domain
CORRECTABLE: dict[str, tuple[str, tuple[Any, ...]]] = {
    "anomaly": ("impact", IMPACTS),
    "dq": ("outlier_flag", (True, False)),
}


class ReviewError(Exception):
    """Domain error carrying an HTTP status and a stable machine code."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class RulesConfigError(ValueError):
    """review_rules.json is missing or malformed."""


# --------------------------------------------------------------------------- rules
def load_rules(project_root: Path) -> dict[str, dict[str, Any]]:
    path = project_root / RULES_FILE
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RulesConfigError(f"{RULES_FILE} not found at the repository root.") from exc
    except (OSError, ValueError) as exc:
        raise RulesConfigError(f"{RULES_FILE} is not valid JSON: {exc}") from exc
    rules = raw.get("rules") if isinstance(raw, dict) else None
    if not isinstance(rules, dict):
        raise RulesConfigError(f"{RULES_FILE}: 'rules' object is required.")
    unknown = set(rules) - set(_KNOWN_RULES)
    if unknown:
        raise RulesConfigError(f"{RULES_FILE}: unknown rule(s) {sorted(unknown)}.")
    for rid in _KNOWN_RULES:
        cfg = rules.get(rid)
        if not isinstance(cfg, dict) or not isinstance(cfg.get("enabled"), bool):
            raise RulesConfigError(f"{RULES_FILE}: {rid}.enabled must be a boolean.")
    for key in ("completeness_below", "dq_score_below"):
        v = rules["R4"].get(key)
        if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float))):
            raise RulesConfigError(f"{RULES_FILE}: R4.{key} must be a number or null.")
    return rules


def _r4_thresholds(cfg: dict[str, Any]) -> dict[str, float]:
    return {
        k: float(cfg[k]) for k in ("completeness_below", "dq_score_below") if cfg.get(k) is not None
    }


def active_rule_ids(rules: dict[str, dict[str, Any]]) -> list[str]:
    out = []
    for rid in _KNOWN_RULES:
        if not rules[rid]["enabled"]:
            continue
        if rid == "R4" and not _r4_thresholds(rules[rid]):
            continue  # no owner-defined threshold: inactive
        out.append(rid)
    return out


def _num(v: Any) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _anomaly_items(doc: dict[str, Any], rules: dict[str, Any]) -> list[tuple[str, str, dict]]:
    wanted = {}
    if rules["R1"]["enabled"]:
        wanted["high"] = "R1"
    if rules["R2"]["enabled"]:
        wanted["medium"] = "R2"
    out = []
    for a in doc.get("anomalies") or []:
        if not isinstance(a, dict):
            continue
        rid = wanted.get(a.get("impact"))  # v1-style entries have no impact -> no item
        if rid and a.get("column") and a.get("method"):
            out.append((f"{a['column']}|{a['method']}", rid, a))
    return out


def _dq_items(doc: dict[str, Any], rules: dict[str, Any]) -> list[tuple[str, str, dict]]:
    th = _r4_thresholds(rules["R4"]) if rules["R4"]["enabled"] else {}
    out = []
    for c in doc.get("columns") or []:
        if not isinstance(c, dict) or not c.get("column"):
            continue
        rid = None
        if rules["R3"]["enabled"] and c.get("outlier_flag") is True:
            rid = "R3"
        elif th:
            comp, score = _num(c.get("completeness")), _num(c.get("dq_score"))
            if (
                "completeness_below" in th and comp is not None and comp < th["completeness_below"]
            ) or ("dq_score_below" in th and score is not None and score < th["dq_score_below"]):
                rid = "R4"
        if rid:
            out.append((str(c["column"]), rid, c))
    return out


# --------------------------------------------------------------------------- sync
_OUTCOME_MESSAGES = {
    prov.BINDING_UNVERIFIABLE: "No provenance record binds these artifacts to the run.",
    prov.UNMANAGED_WRITE: "The artifact was last written outside the API (CLI); not trusted.",
    prov.ARTIFACT_CHANGED: "The artifact no longer matches the fingerprint recorded for the run.",
    prov.ARTIFACT_UNAVAILABLE: "The artifact file could not be read.",
    prov.RUN_ACTIVE: "A run or write is in progress for this project; retry later.",
    prov.RUN_MISMATCH: "The artifact on disk belongs to a different run.",
}


def sync_project(project_root: Path, project: str) -> dict[str, Any]:
    try:
        project_dir = _safe_project_dir(project_root, project)
    except (ValueError, PermissionError) as exc:
        raise ReviewError(422, "invalid_project", "Invalid project name.") from exc
    try:
        rules = load_rules(project_root)
    except RulesConfigError as exc:
        raise ReviewError(500, "rules_config_invalid", str(exc)) from exc
    try:
        with prov.project_write_lease(project_dir):
            return _sync_locked(project_dir, project, rules)
    except prov.ProjectBusyError as exc:
        raise ReviewError(409, prov.RUN_ACTIVE, _OUTCOME_MESSAGES[prov.RUN_ACTIVE]) from exc


def _sync_locked(project_dir: Path, project: str, rules: dict[str, Any]) -> dict[str, Any]:
    if store.get_active_run(project):
        raise ReviewError(409, prov.RUN_ACTIVE, _OUTCOME_MESSAGES[prov.RUN_ACTIVE])
    run = store.latest_completed_run(project)
    if run is None:
        raise ReviewError(404, "no_completed_run", "The project has no completed run.")
    run_id = run["run_id"]
    names = store.run_artifact_names(project, run_id)
    if not names:
        raise ReviewError(
            409, prov.BINDING_UNVERIFIABLE, _OUTCOME_MESSAGES[prov.BINDING_UNVERIFIABLE]
        )
    parsed: list[tuple[str, str, dict[str, Any]]] = []  # (name, sha, doc)
    for name in names:
        res = prov.check_binding(project_dir, project, name, run_id)
        if res.outcome != prov.VERIFIED:
            raise ReviewError(
                409, res.outcome, _OUTCOME_MESSAGES.get(res.outcome, "Provenance check failed.")
            )
        try:
            doc = json.loads(res.data)  # the very bytes that were hash-validated
        except ValueError as exc:
            raise ReviewError(
                409, prov.ARTIFACT_UNAVAILABLE, "Artifact is not valid JSON."
            ) from exc
        if isinstance(doc, dict):
            parsed.append((name, res.row["sha256"], doc))
    # A ref only identifies an item within one source artifact. When a run holds several
    # artifacts of the same kind, qualify refs with the source so equal column names from
    # different datasets do not collapse into one item.
    per_kind = {
        "anomaly": sum(1 for n, _, _ in parsed if n.startswith(ANOMALY_PREFIX)),
        "dq": sum(1 for n, _, _ in parsed if n.startswith(DQ_PREFIX)),
    }
    items: dict[tuple[str, str], dict[str, Any]] = {}
    for name, sha, doc in parsed:
        if name.startswith(ANOMALY_PREFIX):
            kind, found, prefix = "anomaly", _anomaly_items(doc, rules), ANOMALY_PREFIX
        elif name.startswith(DQ_PREFIX):
            kind, found, prefix = "dq", _dq_items(doc, rules), DQ_PREFIX
        else:
            continue
        source = name[len(prefix) :].removesuffix(".json")
        for ref, rid, original in found:
            if per_kind[kind] > 1:
                ref = f"{source}::{ref}"
            items.setdefault(
                (kind, ref),
                {
                    "kind": kind,
                    "ref": ref,
                    "rule_id": rid,
                    "original_json": json.dumps(original, sort_keys=True, ensure_ascii=False),
                    "artifact_sha256": sha,
                },
            )
    created, existing = store.insert_review_items(project, run_id, list(items.values()))
    return {
        "project": project,
        "run_id": run_id,
        "created": created,
        "existing": existing,
        "active_rules": active_rule_ids(rules),
    }


# --------------------------------------------------------------------------- views
def correction_overlay(item: dict[str, Any]) -> dict[str, Any] | None:
    """Latest correction, only while the item is CORRECTED (history is never deleted)."""
    if item["status"] != "CORRECTED":
        return None
    row = store.latest_review_correction(item["id"])
    return json.loads(row["payload_json"]) if row else None


def item_view(item: dict[str, Any], detail: bool = False) -> dict[str, Any]:
    out = {
        "id": item["id"],
        "project": item["project"],
        "run_id": item["run_id"],
        "kind": item["kind"],
        "ref": item["ref"],
        "rule_id": item["rule_id"],
        "status": item["status"],
        "superseded": bool(item["superseded"]),
        "snapshot_verified": bool(item["snapshot_verified"]),
        "artifact_provenance_verified": bool(item["artifact_provenance_verified"]),
        "created_at": item["created_at"],
        "updated_at": item["updated_at"],
    }
    if detail:
        original = json.loads(item["original_json"])
        corr = correction_overlay(item)
        effective = dict(original)
        if corr:
            effective[corr["field"]] = corr["to_value"]
        out.update(
            original=original,
            effective=effective,
            correction=corr,
            artifact_sha256=item["artifact_sha256"],
        )
    return out


# --------------------------------------------------------------------------- decisions
def decide(item_id: int, req: Any) -> tuple[dict[str, Any], dict[str, Any], bool]:
    item = store.get_review_item(item_id)
    if item is None:
        raise ReviewError(404, "not_found", "Review item not found.")
    # Integrity, not history: superseded items stay decidable; tampered/legacy ones do not.
    integrity_error = None
    if not item["snapshot_verified"]:
        integrity_error = (
            "snapshot_unverified",
            "The stored snapshot of this item failed its integrity check; decisions are blocked.",
        )
    elif not item["artifact_provenance_verified"]:
        integrity_error = (
            "provenance_unverified",
            "The source artifact of this item is not recorded for its run; decisions are blocked.",
        )
    correction_json = None
    if req.decision == "CORRECTED":
        field, domain = CORRECTABLE[item["kind"]]
        c = req.correction
        original = json.loads(item["original_json"])
        if c.field != field:
            raise ReviewError(422, "invalid_correction", f"Only '{field}' can be corrected.")
        if c.to_value not in domain or type(c.to_value) is not type(domain[0]):
            raise ReviewError(422, "invalid_correction", f"'{field}' must be one of {domain}.")
        if c.to_value == original.get(field):
            raise ReviewError(422, "invalid_correction", "Correction equals the original value.")
        correction_json = json.dumps(
            {
                "field": field,
                "from_value": original.get(field),
                "to_value": c.to_value,
                "justification": c.justification or req.reason,
            },
            ensure_ascii=False,
        )
    try:
        event, replayed = store.apply_review_decision(
            item_id,
            req.decision,
            req.expected_status,
            req.reviewer,
            req.reason,
            req.client_request_id,
            correction_json,
            integrity_error=integrity_error,
        )
    except store.ReviewNotFoundError as exc:
        raise ReviewError(404, "not_found", "Review item not found.") from exc
    except store.ReviewConflictError as exc:
        raise ReviewError(409, exc.code, str(exc)) from exc
    fresh = store.get_review_item(item_id)
    public_event = {k: v for k, v in event.items() if k != "client_request_id"}
    return item_view(fresh, detail=True), public_event, replayed
