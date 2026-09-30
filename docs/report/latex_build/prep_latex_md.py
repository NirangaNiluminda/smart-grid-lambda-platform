"""Prepares EC8203_Report_DRAFT.md for LaTeX: drops the hand-written title
block (pandoc's own title page replaces it), swaps each ```mermaid fenced
block for a reference to its pre-rendered PDF in diagrams/, and rewrites
screenshots/*.png image references (relative, so they render on GitHub too)
to the absolute path under ../screenshots/, so the build works regardless of
pandoc's working directory. Run from this directory:
`python3 prep_latex_md.py`, then see README.md for the pandoc call.
"""

import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "EC8203_Report_DRAFT.md"
OUT = HERE / "report_for_latex.md"
DIAGRAMS_DIR = HERE / "diagrams"
SCREENSHOTS_DIR = HERE.parent / "screenshots"

text = SRC.read_text(encoding="utf-8")

# Drop the hand-written title block: pandoc's own title page, built from
# -M title/author/date in the build command, replaces it.
text = "## 1. Introduction" + text.split("## 1. Introduction", 1)[1]

captions = [
    "System context: simulated sources, the data platform, and consumers.",
    "One simulated day, sequenced: streaming ingestion through to the daily batch reconciliation and serving.",
    "Deployment: Docker Compose service layout (Part 1 base + Part 2 overlay).",
]

counter = {"i": 0}


def replace_mermaid(m: re.Match) -> str:
    i = counter["i"]
    counter["i"] += 1
    img_path = DIAGRAMS_DIR / f"diagram{i + 1}.pdf"
    cap = captions[i] if i < len(captions) else f"Diagram {i + 1}"
    return f"![{cap}]({img_path}){{ width=95% }}"


text = re.sub(r"```mermaid\n(.*?)```", replace_mermaid, text, flags=re.DOTALL)

text, n_shots = re.subn(
    r"\]\(screenshots/([^)]+)\)",
    lambda m: f"]({SCREENSHOTS_DIR / m.group(1)})",
    text,
)

OUT.write_text(text, encoding="utf-8")
print(
    "wrote", OUT,
    "- replaced", counter["i"], "mermaid blocks with images,",
    n_shots, "screenshot paths resolved",
)
