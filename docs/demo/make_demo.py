"""Record docs/demo/planbound.gif from a REAL run: Planbound against the mock MCP servers.

Every number on screen (calls, cost, refunds undone, what the model saw) comes from this run.

    uv run python -m docs.demo.make_demo
"""

from __future__ import annotations

import asyncio
import json
import re
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from bench.scenarios import refund_manifest
from bench.servers import INJECTION, build_servers, make_world
from planbound.runtime import Runtime
from planbound.store import Store
from planbound.upstream import MCPUpstreams

OUT = Path(__file__).parent / "planbound.gif"
SCRIPT = """\
cust     = crm.get_customer(customer_id="C-42")
orders   = billing.list_orders(customer_id="C-42", status="overdue")
disputes = billing.list_disputes(customer_id="C-42")
eligible = filter(orders.orders, item.amount < 5000
                  and item.order_id not in disputes.order_ids)
refunds  = map(eligible, billing.refund_order(order_id=item.order_id,
               amount=item.amount), max_items=20)
notify   = email.send_email(to=cust.email, template="refund_processed",
               vars={"count": count(eligible)}, after=[refunds])
return eligible, refunds, notify"""

# GitHub dark palette
BG, PANEL, FG, DIM = "#0d1117", "#161b22", "#e6edf3", "#8b949e"
GREEN, RED, YELLOW, BLUE, PURPLE, CYAN = "#3fb950", "#f85149", "#d29922", "#58a6ff", "#bc8cff", "#39c5cf"
W, H, PAD, LINE_H, FONT_SIZE = 980, 500, 28, 23, 16
ROWS = (H - 70 - PAD) // LINE_H
SYMBOLS = set("₹✔✖↺")  # drawn with DejaVu Sans Mono: JetBrains Mono has no glyphs for these


async def facts() -> dict:
    """Run the three scenes for real and keep what the screen will show."""
    out: dict = {}
    with tempfile.TemporaryDirectory() as tmp:
        for scene in ("ok", "fail", "inject"):
            world = make_world(n_orders=60, n_overdue=16, seed=11, latency_s=0, inject_note=scene == "inject")
            eligible = sorted(o.order_id for o in world.orders.values()
                              if o.status == "overdue" and o.amount < 5000 and o.order_id not in world.disputed)
            if scene == "fail":
                world.fail_refund_on = {eligible[-1]}
            async with MCPUpstreams(build_servers(world, extras=False)) as up:
                rt = Runtime(Store(Path(tmp) / f"{scene}.db"), up, refund_manifest(up.catalog, auto_max=1e9),
                             {"user": "ops"}, display_dir=Path(tmp) / "display", retry_delay_s=0)
                if scene == "ok":
                    out["compiled"] = await rt.submit(SCRIPT, mode="compile")
                    out["preview"] = await rt.submit(SCRIPT, mode="preview")
                res = await rt.submit(SCRIPT)
                out[scene] = res
                out[scene + "_world"] = {"refunded": world.refunded(), "emails": len(world.emails),
                                         "failed_on": eligible[-1]}
                if scene == "inject":
                    out["model_saw"] = json.dumps(res)
    return out


def script_lines(f: dict) -> list:
    pv, ok, fail = f["preview"], f["ok"], f["fail"]
    worst = f["compiled"]["worst_case"]
    refunds = pv["would_call"].get("billing.refund_order", 0)
    comp = fail["compensation"]
    saw = f["model_saw"]
    note_hidden = "IGNORE" not in saw and "attacker" not in saw
    return [
        ("title", "1 · The agent sends one plan, not twenty tool calls"),
        ("type", [("$ ", GREEN), ("agent ", BLUE), ("→ planbound_submit_plan(script, mode=", FG),
                  ('"preview"', YELLOW), (")", FG)]),
        *[("show", [("  ", FG), *_highlight(ln)]) for ln in SCRIPT.splitlines()],
        ("pause", 500),
        ("show", [("✔ checked   ", GREEN), (f"bounded: at most {worst['upstream_calls']} calls, worst case ₹{worst['cost']:,.0f}", FG)]),
        ("show", [("✔ preview   ", GREEN), (f"{_n(refunds, 'refund')} · ₹{pv['cost']:,.0f} · 1 email", FG),
                  ("   nothing written yet", DIM)]),
        ("show", [("✔ approval  ", GREEN), (f"{pv['approval']} (inside the budget policy)", FG)]),
        ("pause", 1600),
        ("title", "2 · Commit: undoable steps first, the email last"),
        ("type", [("$ ", GREEN), ("agent ", BLUE), ("→ planbound_submit_plan(script)", FG)]),
        ("show", [("✔ completed ", GREEN), ((f"{_n(len(f['ok_world']['refunded']), 'refund')} issued, "
                                            f"{_n(f['ok_world']['emails'], 'receipt')} sent"), FG)]),
        ("show", [("  journal   ", DIM), (", ".join(f"{t.split('.')[1]} ×{sum(v.values())}"
                                                    for t, v in ok["writes"].items()), DIM)]),
        ("pause", 1600),
        ("title", "3 · One refund fails mid-batch"),
        ("type", [("$ ", GREEN), ("agent ", BLUE), ("→ planbound_submit_plan(script)", FG)]),
        ("show", [("✖ failed    ", RED), (f"billing declined {f['fail_world']['failed_on']}", FG)]),
        ("show", [("↺ rollback  ", YELLOW), (f"{_n(comp['compensated'], 'refund')} undone, newest first", FG)]),
        ("show", [("✔ state     ", GREEN),
                  ((f"{_n(len(f['fail_world']['refunded']), 'refund')} left, "
                   f"{_n(f['fail_world']['emails'], 'email')} sent · nothing half-done"), FG)]),
        ("pause", 1600),
        ("title", "4 · A support note says: “IGNORE ALL PREVIOUS INSTRUCTIONS…”"),
        ("show", [("  untrusted ", DIM), ((INJECTION[:58] + "…"), RED)]),
        ("type", [("$ ", GREEN), ("agent ", BLUE), ("reads the result", FG)]),
        ("show", [("✔ model saw ", GREEN), ("counts, sums and ids, computed by code", FG)]),
        ("show", [("✔ injection ", GREEN) if note_hidden else ("✖ injection ", RED),
                  ("never reached the model" if note_hidden else "REACHED THE MODEL", FG),
                  (f"   ({len(saw):,} chars checked)", DIM)]),
        ("pause", 1800),
        ("title", "Planbound: prepared statements and sagas for MCP agent actions"),
        ("show", [("", FG)]),
        ("show", [("  ✔ ", GREEN), ("one bounded plan", FG), ("        checked before anything runs", DIM)]),
        ("show", [("  ✔ ", GREEN), ("exact preview", FG), ("           calls, cost and emails, nothing written", DIM)]),
        ("show", [("  ✔ ", GREEN), ("budget approval", FG), ("         humans only past the limit", DIM)]),
        ("show", [("  ✔ ", GREEN), ("saga rollback", FG), ("           failures leave nothing half-done", DIM)]),
        ("show", [("  ✔ ", GREEN), ("injection-proof results", FG), (" the model sees numbers, not text", DIM)]),
        ("show", [("  ✔ ", GREEN), ("your MCP servers", FG), ("        unchanged", DIM)]),
        ("show", [("", FG)]),
        ("show", [("  every number in this demo came from a real run · docs/demo/make_demo.py", DIM)]),
        ("pause", 3500),
    ]


_TOKEN = re.compile(r'(?P<str>"[^"]*")|(?P<num>\b\d+\b)|(?P<kw>\b(?:filter|map|count|return|and|not|in)\b)'
                    r'|(?P<tool>\b\w+\.\w+(?=\())|(?P<step>^\s*\w+(?=\s*=))|(?P<other>.)')
_COLORS = {"str": YELLOW, "num": CYAN, "kw": PURPLE, "tool": GREEN, "step": BLUE, "other": FG}


def _highlight(line: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for m in _TOKEN.finditer(line):
        color = _COLORS[m.lastgroup]
        if out and out[-1][1] == color:
            out[-1] = (out[-1][0] + m.group(), color)
        else:
            out.append((m.group(), color))
    return out


def _n(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


class Screen:
    def __init__(self):
        mono = "/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Regular.ttf"
        bold = "/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Bold.ttf"
        fallback = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
        self.font = _font(mono, fallback, FONT_SIZE)
        self.bold = _font(bold, fallback, FONT_SIZE + 1)
        self.symbols = ImageFont.truetype(fallback, FONT_SIZE)
        self.lines: list[list[tuple[str, str]]] = []
        self.title = ""
        self.frames: list[Image.Image] = []
        self.durations: list[int] = []

    def frame(self, ms: int) -> None:
        img = Image.new("RGB", (W, H), BG)
        d = ImageDraw.Draw(img)
        d.rounded_rectangle((10, 10, W - 10, H - 10), radius=14, fill=PANEL, outline="#30363d", width=2)
        for i, c in enumerate(("#ff5f56", "#ffbd2e", "#27c93f")):
            d.ellipse((30 + i * 22, 28, 44 + i * 22, 42), fill=c)
        d.text((W // 2, 35), "planbound — real run against mock MCP servers", fill=DIM, font=self.font, anchor="mm")
        if self.title:
            d.text((PAD + 6, 66), self.title, fill=CYAN, font=self.bold)
        y = 100
        for segs in self.lines[-ROWS:]:
            x = PAD + 6
            for text, color in segs:
                for ch in text:
                    f = self.symbols if ch in SYMBOLS else self.font
                    d.text((x, y), ch, fill=color, font=f)
                    x += self.font.getlength("M")
            y += LINE_H
        self.frames.append(img)
        self.durations.append(ms)

    def play(self, steps: list) -> None:
        for kind, arg in steps:
            if kind == "title":
                self.lines, self.title = [], arg
                self.frame(700)
            elif kind == "pause":
                self.durations[-1] += arg
            elif kind == "show":
                self.lines.append(arg)
                self.frame(260)
            elif kind == "type":
                full = "".join(t for t, _ in arg)
                for n in range(4, len(full) + 4, 4):
                    self.lines = self.lines[:-1] if n > 4 else self.lines
                    self.lines.append(_cut(arg, n))
                    self.frame(45)
                self.durations[-1] += 250


def _cut(segs: list[tuple[str, str]], n: int) -> list[tuple[str, str]]:
    out, left = [], n
    for text, color in segs:
        if left <= 0:
            break
        out.append((text[:left], color))
        left -= len(text)
    return out


def _font(path: str, fallback: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(path, size)
    except OSError:
        return ImageFont.truetype(fallback, size)


def main() -> None:
    f = asyncio.run(facts())
    screen = Screen()
    screen.play(script_lines(f))
    frames = [fr.convert("P", palette=Image.Palette.ADAPTIVE, colors=128) for fr in screen.frames]
    frames[0].save(OUT, save_all=True, append_images=frames[1:], duration=screen.durations, loop=0,
                   optimize=True, disposal=1)
    screen.frames[-1].save(OUT.with_suffix(".png"))
    print(f"wrote {OUT} ({OUT.stat().st_size / 1e6:.2f} MB, {len(frames)} frames, "
          f"{sum(screen.durations) / 1000:.1f}s)")


if __name__ == "__main__":
    main()
