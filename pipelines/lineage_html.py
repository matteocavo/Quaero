"""Lineage HTML renderer.

Reads ``metadata/lineage.json`` and generates a self-contained
``metadata/lineage.html`` with an embedded Mermaid.js flowchart.

The diagram groups columns into three subgraphs (RAW → STAGING → MART)
and colour-codes edges by transformation type:
  - passthrough : grey
  - normalized  : blue
  - type_cast   : amber
  - aggregated  : green
"""

from __future__ import annotations

import json
import logging
import re
from html import escape
from pathlib import Path
from typing import Any

from pipelines.utils import resolve_project_root


LOGGER = logging.getLogger(__name__)

_EDGE_COLORS: dict[str, str] = {
    "passthrough": "#6b7280",
    "normalized": "#3b82f6",
    "type_cast": "#f59e0b",
    "aggregated": "#10b981",
}

_LAYER_LABELS: dict[str, str] = {
    "raw": "RAW SOURCE",
    "staging": "STAGING",
    "mart": "MART",
}

_LAYER_NODE_CLASS: dict[str, str] = {
    "raw": "rawNode",
    "staging": "stagingNode",
    "mart": "martNode",
}


def render_lineage_html(
    source_name: str | None = None,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Generate ``metadata/lineage.html`` from ``metadata/lineage.json``.

    Args:
        source_name: Unused; kept for API symmetry with other pipeline modules.
        project_root: Optional project root for file resolution.

    Returns:
        Dict with ``output_path`` (relative to project_root) and optionally
        ``skipped: True`` when lineage.json is absent.
    """
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    lineage_json_path = metadata_dir / "lineage.json"

    if not lineage_json_path.exists():
        LOGGER.warning(
            "lineage.json not found at %s — skipping HTML render", lineage_json_path
        )
        return {"output_path": "metadata/lineage.html", "skipped": True}

    lineage = json.loads(lineage_json_path.read_text(encoding="utf-8"))
    mermaid_code = _build_mermaid(lineage)
    html_content = _build_html(lineage, mermaid_code)

    output_path = metadata_dir / "lineage.html"
    output_path.write_text(html_content, encoding="utf-8")
    LOGGER.info("Lineage HTML written to %s", output_path)

    return {"output_path": output_path.relative_to(root_dir).as_posix()}


# ---------------------------------------------------------------------------
# Mermaid diagram builder
# ---------------------------------------------------------------------------


def _sanitize_id(node_id: str) -> str:
    """Convert a node id like ``raw::col_name`` to a Mermaid-safe identifier."""
    safe_id = node_id.replace("::", "__")
    safe_id = re.sub(r"[^\w]", "_", safe_id)
    if safe_id and safe_id[0].isdigit():
        return f"n_{safe_id}"
    return safe_id or "node"


def _build_mermaid(lineage: dict[str, Any]) -> str:
    """Build a Mermaid ``graph LR`` string from the lineage document."""
    nodes: list[dict[str, Any]] = lineage.get("nodes", [])
    edges: list[dict[str, Any]] = lineage.get("edges", [])

    # Group nodes by layer
    by_layer: dict[str, list[dict[str, Any]]] = {"raw": [], "staging": [], "mart": []}
    for node in nodes:
        layer = node.get("layer", "staging")
        by_layer.setdefault(layer, []).append(node)

    lines = ["graph LR"]

    # Subgraphs per layer (raw → staging → mart)
    for layer in ("raw", "staging", "mart"):
        layer_nodes = by_layer.get(layer, [])
        if not layer_nodes:
            continue
        label = _LAYER_LABELS.get(layer, layer.upper())
        lines.append(f'  subgraph {layer.upper()}["{label}"]')
        for node in layer_nodes:
            safe_id = _sanitize_id(node["id"])
            display = node.get("name", node["id"])
            # Escape double quotes inside the label
            display_escaped = str(display).replace('"', "'")
            lines.append(f'    {safe_id}["{display_escaped}"]')
        lines.append("  end")

    lines.append("")

    # Edges with type labels
    for edge in edges:
        from_id = _sanitize_id(edge["from"])
        to_id = _sanitize_id(edge["to"])
        edge_type = str(edge.get("type", "passthrough")).replace('"', "'")
        lines.append(f'  {from_id} -->|"{edge_type}"| {to_id}')

    lines.append("")

    # Node style classes
    lines.append("  classDef rawNode fill:#f3f4f6,stroke:#9ca3af,color:#111827")
    lines.append("  classDef stagingNode fill:#dbeafe,stroke:#3b82f6,color:#111827")
    lines.append("  classDef martNode fill:#d1fae5,stroke:#10b981,color:#111827")

    for layer, css_class in _LAYER_NODE_CLASS.items():
        ids = [_sanitize_id(n["id"]) for n in by_layer.get(layer, [])]
        if ids:
            lines.append(f"  class {','.join(ids)} {css_class}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML page builder
# ---------------------------------------------------------------------------


def _build_html(lineage: dict[str, Any], mermaid_code: str) -> str:
    """Build a self-contained HTML page embedding the Mermaid diagram."""
    source = escape(str(lineage.get("source", "unknown")))
    generated_at = escape(str(lineage.get("generated_at", "")))
    node_count = lineage.get("node_count", 0)
    edge_count = lineage.get("edge_count", 0)

    legend_html = "\n        ".join(
        f'<span class="legend-item" style="border-left: 4px solid {color}">'
        f"{escape(edge_type)}</span>"
        for edge_type, color in _EDGE_COLORS.items()
    )

    # Escape before embedding so lineage labels remain text, not HTML.
    indented_mermaid = "\n".join(
        "      " + escape(line) for line in mermaid_code.splitlines()
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Quaero · Lineage — {source}</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f9fafb;
      color: #111827;
      padding: 2rem;
      line-height: 1.5;
    }}
    h1 {{
      font-size: 1.4rem;
      font-weight: 700;
      margin-bottom: 0.25rem;
      color: #111827;
    }}
    .meta {{
      font-size: 0.78rem;
      color: #6b7280;
      margin-bottom: 1.5rem;
    }}
    .stats {{
      display: flex;
      gap: 1rem;
      margin-bottom: 1.5rem;
      flex-wrap: wrap;
    }}
    .stat {{
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 0.75rem 1.25rem;
      font-size: 0.82rem;
      color: #374151;
    }}
    .stat strong {{
      display: block;
      font-size: 1.5rem;
      font-weight: 700;
      color: #1d4ed8;
      line-height: 1.2;
    }}
    .legend {{
      display: flex;
      gap: 0.75rem;
      margin-bottom: 1.5rem;
      flex-wrap: wrap;
      align-items: center;
    }}
    .legend-label {{
      font-size: 0.75rem;
      color: #6b7280;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .legend-item {{
      font-size: 0.75rem;
      padding: 0.2rem 0.6rem 0.2rem 0.5rem;
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 4px;
      color: #374151;
    }}
    .diagram-container {{
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 2rem;
      overflow-x: auto;
    }}
    .footer {{
      margin-top: 1.5rem;
      font-size: 0.72rem;
      color: #9ca3af;
    }}
  </style>
</head>
<body>
  <h1>Column Lineage — {source}</h1>
  <p class="meta">Generated at {generated_at} &nbsp;·&nbsp; Quaero v0.4</p>

  <div class="stats">
    <div class="stat"><strong>{node_count}</strong>columns tracked</div>
    <div class="stat"><strong>{edge_count}</strong>transformations</div>
  </div>

  <div class="legend">
    <span class="legend-label">Edge types</span>
    {legend_html}
  </div>

  <div class="diagram-container">
    <div class="mermaid">
{indented_mermaid}
    </div>
  </div>

  <p class="footer">
    Generated by Quaero · pipelines/lineage_html.py ·
    <a href="https://github.com/matteocavo/Quaero" style="color:#6b7280">github.com/matteocavo/Quaero</a>
  </p>

  <script>
    mermaid.initialize({{
      startOnLoad: true,
      theme: "neutral",
      flowchart: {{ useMaxWidth: true, htmlLabels: true }}
    }});
  </script>
</body>
</html>
"""
