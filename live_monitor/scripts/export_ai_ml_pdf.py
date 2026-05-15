"""
Build AI_ML_PIPELINE.pdf from AI_ML_PIPELINE.md.

Requires: pip install markdown playwright pillow
          python -m playwright install chromium
          Node.js (npx) for @mermaid-js/mermaid-cli
"""

from __future__ import annotations

import asyncio
import base64
import html
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
MD_PATH = ROOT / "AI_ML_PIPELINE.md"
OUT_PDF = ROOT / "AI_ML_PIPELINE.pdf"
MERMAID_CFG = SCRIPTS / "mermaid-pdf-config.json"

BlockType = Literal[
    "intro", "h2", "h3", "paragraph", "table", "code", "mermaid", "footer"
]


@dataclass
class Block:
    kind: BlockType
    text: str = ""
    lang: str = ""
    diagram_idx: int = -1


@dataclass
class Document:
    title: str
    subtitle: str
    blocks: list[Block] = field(default_factory=list)


def parse_document(md: str) -> Document:
    lines = md.splitlines()
    doc = Document(
        title="KI- & ML-Pipeline — Extruder Live-Monitoring",
        subtitle="PM-Project · Datenfluss, Modelle, Layer 1 & Layer 2, Training, Roadmap",
    )
    i, n = 0, len(lines)
    diagram_idx = 0
    got_intro = False

    while i < n:
        line = lines[i]
        stripped = line.strip()

        if not stripped or stripped == "---":
            i += 1
            continue

        if stripped.startswith("# ") and not got_intro:
            i += 1
            continue

        if stripped.startswith("## "):
            doc.blocks.append(Block("h2", stripped[3:].strip()))
            i += 1
            continue

        if stripped.startswith("### "):
            doc.blocks.append(Block("h3", stripped[4:].strip()))
            i += 1
            continue

        if stripped.startswith("```"):
            lang = stripped[3:].strip() or "text"
            i += 1
            body: list[str] = []
            while i < n and not lines[i].strip().startswith("```"):
                body.append(lines[i])
                i += 1
            content = "\n".join(body)
            if lang == "mermaid":
                doc.blocks.append(Block("mermaid", content, diagram_idx=diagram_idx))
                diagram_idx += 1
            else:
                doc.blocks.append(Block("code", content, lang=lang))
            i += 1
            continue

        if stripped.startswith("|"):
            rows: list[str] = []
            while i < n and lines[i].strip().startswith("|"):
                rows.append(lines[i])
                i += 1
            doc.blocks.append(Block("table", "\n".join(rows)))
            continue

        if stripped.startswith("*") and "aktualisiert" in stripped.lower():
            doc.blocks.append(Block("footer", stripped.strip("* ").strip()))
            i += 1
            continue

        para: list[str] = []
        while i < n:
            s = lines[i].strip()
            if not s or s == "---":
                break
            if s.startswith("#") or s.startswith("|") or s.startswith("```"):
                break
            para.append(lines[i])
            i += 1
        if para:
            kind: BlockType = "intro" if not got_intro else "paragraph"
            doc.blocks.append(Block(kind, "\n".join(para)))
            if kind == "intro":
                got_intro = True
        else:
            i += 1

    return doc


def _mermaid_head(code: str) -> str:
    return code.strip().split("\n", 1)[0].strip().lower()


def _mermaid_render_params(code: str) -> tuple[str, str]:
    """Return (width, scale) tuned per diagram type."""
    head = _mermaid_head(code)
    n_lines = len([ln for ln in code.splitlines() if ln.strip() and not ln.strip().startswith("%%")])

    if head.startswith("flowchart lr") or head.startswith("graph lr"):
        if n_lines <= 6:
            return "800", "2.2"
        return "1100", "1.75"
    if head.startswith("sequencediagram"):
        return "1200", "1.7"
    if head.startswith("timeline"):
        return "1200", "1.65"
    if head.startswith("erdiagram"):
        return "950", "1.75"
    if head.startswith("flowchart td") or head.startswith("graph td"):
        return "1100", "1.75"
    return "1100", "1.8"


def _diagram_class(code: str) -> str:
    """CSS class from Mermaid type (not pixel size)."""
    head = _mermaid_head(code)
    n_lines = len([ln for ln in code.splitlines() if ln.strip()])

    if head.startswith("flowchart lr") or head.startswith("graph lr"):
        return "fig-lr-small" if n_lines <= 6 else "fig-lr"
    if head.startswith("sequencediagram"):
        return "fig-seq"
    if head.startswith("timeline"):
        return "fig-timeline"
    if head.startswith("erdiagram"):
        return "fig-er"
    if head.startswith("flowchart td") or head.startswith("graph td"):
        return "fig-td"
    return "fig-tb"


def render_diagrams(doc: Document, work: Path) -> list[Path]:
    mmd_dir = work / "mmd"
    png_dir = work / "png"
    mmd_dir.mkdir(parents=True, exist_ok=True)
    png_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []

    for block in doc.blocks:
        if block.kind != "mermaid":
            continue
        idx = block.diagram_idx
        mmd = mmd_dir / f"d{idx}.mmd"
        png = png_dir / f"d{idx}.png"
        mmd.write_text(block.text, encoding="utf-8")
        w, scale = _mermaid_render_params(block.text)
        cmd = (
            f'npx -y @mermaid-js/mermaid-cli@10.9.0 -i "{mmd}" -o "{png}" '
            f'-c "{MERMAID_CFG}" -b white -w {w} -s {scale}'
        )
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=200)
        if proc.returncode != 0:
            raise RuntimeError(f"Diagram {idx + 1} failed:\n{proc.stderr[-600:]}")
        paths.append(png)
    return paths


def _md_fragment(text: str) -> str:
    import markdown

    return markdown.markdown(text, extensions=["tables", "fenced_code"], output_format="html5")


def _img_data_uri(png: Path) -> str:
    return "data:image/png;base64," + base64.b64encode(png.read_bytes()).decode("ascii")


PRINT_CSS = """
@page { size: A4; margin: 14mm 13mm 16mm 13mm; }
html, body { margin: 0; padding: 0; }
body {
  font-family: "Segoe UI", Calibri, Arial, sans-serif;
  font-size: 9.75pt;
  line-height: 1.42;
  color: #1a1625;
  -webkit-print-color-adjust: exact;
  print-color-adjust: exact;
}
.cover {
  background: linear-gradient(120deg, #4c1d95 0%, #7c3aed 55%, #8b5cf6 100%);
  color: #fff;
  padding: 22px 24px 20px;
  margin: 0 0 14px;
}
.cover h1 { margin: 0 0 6px; font-size: 19pt; font-weight: 700; border: none; }
.cover p { margin: 0; font-size: 10pt; opacity: 0.94; }
.chapter { margin: 0 0 6px; }
.chapter > h2 {
  color: #5b21b6;
  font-size: 13.5pt;
  font-weight: 700;
  margin: 14px 0 7px;
  padding: 0 0 5px;
  border-bottom: 2px solid #c4b5fd;
  break-after: avoid;
}
.chapter:first-child > h2 { margin-top: 2px; }
h3 {
  color: #6d28d9;
  font-size: 11pt;
  font-weight: 600;
  margin: 10px 0 5px;
  break-after: avoid;
}
.lead { font-size: 10.25pt; margin: 0 0 10px; color: #312e81; }
.lead p { margin: 0; }
p { margin: 0 0 6px; }
ul, ol { margin: 0 0 7px; padding-left: 1.15em; }
li { margin: 2px 0; }
strong { color: #4c1d95; }
code {
  font-family: Consolas, monospace;
  font-size: 8.75pt;
  background: #f5f3ff;
  color: #4c1d95;
  padding: 1px 4px;
  border-radius: 3px;
}
pre {
  background: #2e1065;
  color: #ede9fe;
  font-family: Consolas, monospace;
  font-size: 8.25pt;
  line-height: 1.35;
  padding: 9px 11px;
  border-radius: 5px;
  margin: 5px 0 9px;
  white-space: pre-wrap;
  word-break: break-word;
  break-inside: avoid;
}
table {
  width: 100%;
  border-collapse: collapse;
  margin: 5px 0 9px;
  font-size: 9pt;
  break-inside: avoid;
}
thead th {
  background: #6d28d9;
  color: #fff;
  font-weight: 600;
  text-align: left;
  padding: 5px 8px;
  border: 1px solid #5b21b6;
}
tbody td {
  padding: 4px 8px;
  border: 1px solid #ddd6fe;
  vertical-align: top;
}
tbody tr:nth-child(even) td { background: #faf5ff; }
figure.fig { margin: 4px 0 10px; padding: 0; text-align: center; break-inside: avoid; }
figure.fig img { display: block; margin: 0 auto; object-fit: contain; }
/* Vertical flowcharts + ER: use page width */
figure.fig-tb img,
figure.fig-er img {
  width: 100%;
  max-width: 100%;
  height: auto;
  max-height: 138mm;
}
figure.fig-td img {
  width: 100%;
  max-width: 100%;
  height: auto;
  max-height: 108mm;
}
/* Wide diagrams: fix height so they stay readable (width:auto scales up) */
figure.fig-lr img,
figure.fig-timeline img {
  height: 78mm;
  width: auto;
  max-width: 100%;
}
figure.fig-lr-small img {
  height: 52mm;
  width: auto;
  max-width: 92%;
}
figure.fig-seq img {
  height: 82mm;
  width: auto;
  max-width: 100%;
}
.doc-footer {
  margin-top: 12px;
  padding-top: 8px;
  border-top: 1px solid #e9d5ff;
  font-size: 8.5pt;
  color: #6b7280;
  font-style: italic;
}
"""


def build_html(doc: Document, pngs: list[Path]) -> str:
    import markdown

    out: list[str] = [
        "<!DOCTYPE html>",
        "<html lang='de'><head><meta charset='utf-8'/>",
        f"<title>{html.escape(doc.title)}</title>",
        f"<style>{PRINT_CSS}</style></head><body>",
        "<header class='cover'>",
        f"<h1>{html.escape(doc.title)}</h1>",
        f"<p>{html.escape(doc.subtitle)}</p>",
        "</header>",
        "<div class='content'>",
    ]

    in_chapter = False
    png_i = 0

    for block in doc.blocks:
        if block.kind == "h2":
            if in_chapter:
                out.append("</section>")
            out.append("<section class='chapter'>")
            out.append(f"<h2>{html.escape(block.text)}</h2>")
            in_chapter = True
        elif block.kind == "h3":
            out.append(f"<h3>{html.escape(block.text)}</h3>")
        elif block.kind == "intro":
            out.append(f"<div class='lead'>{_md_fragment(block.text)}</div>")
        elif block.kind == "paragraph":
            out.append(_md_fragment(block.text))
        elif block.kind == "table":
            out.append(markdown.markdown(block.text, extensions=["tables"]))
        elif block.kind == "code":
            code = html.escape(block.text)
            lang = html.escape(block.lang)
            out.append(f"<pre><code class='lang-{lang}'>{code}</code></pre>")
        elif block.kind == "mermaid":
            png = pngs[png_i]
            png_i += 1
            cls = _diagram_class(block.text)
            uri = _img_data_uri(png)
            num = block.diagram_idx + 1
            out.append(
                f"<figure class='fig {cls}'>"
                f"<img src='{uri}' alt='Diagramm {num}'/>"
                "</figure>"
            )
        elif block.kind == "footer":
            out.append(f"<div class='doc-footer'>{_md_fragment(block.text)}</div>")

    if in_chapter:
        out.append("</section>")
    out.append("</div></body></html>")
    return "\n".join(out)


async def print_pdf(html_path: Path, pdf_path: Path) -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page = await browser.new_page()
        await page.goto(html_path.resolve().as_uri(), wait_until="load")
        await page.pdf(
            path=str(pdf_path),
            format="A4",
            print_background=True,
            prefer_css_page_size=True,
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
        )
        await browser.close()


def main() -> None:
    if not MD_PATH.is_file():
        sys.exit(f"Not found: {MD_PATH}")

    doc = parse_document(MD_PATH.read_text(encoding="utf-8"))

    with tempfile.TemporaryDirectory(prefix="pm_pdf_") as tmp:
        pngs = render_diagrams(doc, Path(tmp))
        html_path = ROOT / "_pdf_print.html"
        html_path.write_text(build_html(doc, pngs), encoding="utf-8")

        for target in (OUT_PDF, ROOT / "AI_ML_PIPELINE_new.pdf"):
            try:
                asyncio.run(print_pdf(html_path, target))
                print(f"PDF: {target}")
                print(f"HTML preview: {html_path}")
                return
            except PermissionError:
                continue

        fallback = ROOT / "AI_ML_PIPELINE_new.pdf"
        asyncio.run(print_pdf(html_path, fallback))
        print(f"PDF (fallback): {fallback}")
        print(f"HTML preview: {html_path}")


if __name__ == "__main__":
    main()
