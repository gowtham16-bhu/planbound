"""Draw docs/demo/latency-{light,dark}.svg from bench/results.json (run `python -m bench.run` first).

    uv run python -m docs.demo.make_chart
"""

from __future__ import annotations

import json
from html import escape
from pathlib import Path

HERE = Path(__file__).parent
WORKLOAD = "refund_40_orders"
LABELS = {
    "Direct MCP (parallel calls)": "Direct MCP",
    "MCP code mode": "Code mode",
    "Planbound (JSON plan)": "Planbound · JSON plan",
    "Planbound (script)": "Planbound · script",
    "Planbound (prepared, repeat run)": "Planbound · prepared",
}
THEMES = {
    "light": {"fg": "#1f2328", "dim": "#59636e", "grid": "#d1d9e0", "other": "#8c959f", "pb": "#0969da",
              "best": "#1a7f37", "bad": "#cf222e"},
    "dark": {"fg": "#e6edf3", "dim": "#9198a1", "grid": "#3d444d", "other": "#6e7681", "pb": "#4493f8",
             "best": "#3fb950", "bad": "#f85149"},
}


def chart(rows: list[dict], t: dict) -> str:
    w, left, right, top, bar_h, gap = 760, 190, 150, 54, 26, 14
    h = top + len(rows) * (bar_h + gap) + 30
    scale = (w - left - right) / max(r["modeled_latency_s"] for r in rows)
    best = min(r["modeled_latency_s"] for r in rows)
    out = [(f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
           f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif">'),
           (f'<text x="0" y="20" font-size="16" font-weight="600" fill="{t["fg"]}">'
           'Modeled latency: refund 40 orders, email one receipt</text>'),
           (f'<text x="0" y="40" font-size="12" fill="{t["dim"]}">simulated model, real MCP calls · '
           'output tokens on the right · lower is better</text>')]
    for i, r in enumerate(rows):
        y = top + i * (bar_h + gap)
        name = LABELS.get(r["strategy"], r["strategy"])
        planbound = name.startswith("Planbound")
        color = t["best"] if r["modeled_latency_s"] == best else (t["pb"] if planbound else t["other"])
        width = max(2, r["modeled_latency_s"] * scale)
        weight = "600" if planbound else "400"
        out += [
            (f'<text x="{left - 12}" y="{y + bar_h * 0.68}" font-size="13" text-anchor="end" '
            f'font-weight="{weight}" fill="{t["fg"]}">{escape(name)}</text>'),
            f'<rect x="{left}" y="{y}" width="{width:.1f}" height="{bar_h}" rx="5" fill="{color}"/>',
            (f'<text x="{left + width + 8:.1f}" y="{y + bar_h * 0.68}" font-size="13" font-weight="600" '
            f'fill="{t["fg"]}">{r["modeled_latency_s"]:.1f}s</text>'),
            (f'<text x="{w - 4}" y="{y + bar_h * 0.68}" font-size="12" text-anchor="end" fill="{t["dim"]}">'
            f'{r["output_tokens"]} tok out</text>'),
        ]
        if "PARTIAL" in r.get("end_state", ""):
            out.append(f'<text x="{left + width + 50:.1f}" y="{y + bar_h * 0.68}" font-size="12" '
                       f'fill="{t["bad"]}">left half-done</text>')
    out.append("</svg>")
    return "\n".join(out)


def main() -> None:
    rows = json.loads(Path("bench/results.json").read_text())[WORKLOAD]
    for name, theme in THEMES.items():
        path = HERE / f"latency-{name}.svg"
        path.write_text(chart(rows, theme) + "\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
