# Rebuilding `EC8203_Report.pdf`

The report is authored in `../EC8203_Report_DRAFT.md` and compiled to PDF via
pandoc + LaTeX (not a Markdown-to-HTML-to-PDF route), because the assignment
report is a formal academic PDF and LaTeX gives proper section numbering, a
generated table of contents, and clean typeset tables/figures.

The three architecture diagrams are authored once as Mermaid source
(`diagrams/diagram{1,2,3}.mmd`, matching §3.1/§3.3/§3.5 of the report) and
pre-rendered to vector PDF — LaTeX cannot render Mermaid itself, and rendering
to PDF (not PNG) keeps them crisp at any zoom in the final document.

## Prerequisites

- `pandoc` (a self-contained binary is enough: `pip install pypandoc-binary`
  gives you one with no root/apt required)
- A LaTeX distribution with `pdflatex` (this repo was built against
  `texlive`; the `fvextra` package must be available for code-block line
  wrapping — it ships in `texlive-latex-extra`)
- Only needed if you edit the diagrams: Node + `npx`, and Chrome/Chromium
  installed locally (mermaid-cli drives it instead of downloading its own):
  ```bash
  PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=1 PUPPETEER_EXECUTABLE_PATH=$(which google-chrome) \
    npx --yes @mermaid-js/mermaid-cli@10.9.1 -i diagrams/diagram1.mmd -o diagrams/diagram1.pdf -b white --pdfFit
  # repeat for diagram2.mmd, diagram3.mmd
  ```

## Build

```bash
cd docs/report/latex_build
python3 prep_latex_md.py   # writes report_for_latex.md (title block dropped, diagrams substituted)
python3 - <<'PY'
import pypandoc
pypandoc.convert_file(
    "report_for_latex.md", "pdf", outputfile="../EC8203_Report.pdf",
    extra_args=[
        "--pdf-engine=pdflatex",
        "--toc", "--toc-depth=2",
        "-V", "geometry:margin=1in",
        "-V", "papersize=a4",
        "-V", "fontsize=11pt",
        "-V", "colorlinks=true",
        "-V", "linkcolor=NavyBlue",
        "-V", "urlcolor=NavyBlue",
        "-V", "toccolor=black",
        "-V", "title=Smart Grid Energy Monitoring \\\\& Billing",
        "-V", "subtitle=A Lambda Architecture Data Platform",
        "-V", "author=Niluminda G.W.N, EG/2021/4691 (Dev A) \\\\and Niroshan I.J, EG/2021/4695 (Dev B) \\\\and Kavinda S.O, EG/2021/4608 (Dev C)",
        "-V", "date=EC8203 Applied Big Data Engineering - Mini Project Report",
        "--syntax-highlighting=tango",
        "--include-in-header=header.tex",
    ],
)
PY
```

`header.tex` adds `microtype` + a modest `\emergencystretch` (keeps long
identifiers like `daily_energy_reconciliation` from overflowing the margin in
running text) and `fvextra` line-wrapping for fenced code blocks (so the
`services/<name>/src/<pkg>/` tree in §3.4 wraps instead of clipping at the
page edge).

## Screenshots

§9 Results references 6 named slots — `../screenshots/01-compose-ps.png`
through `06-billing-report-html.png` (all filled with real captures) — each
right after the paragraph it illustrates. `prep_latex_md.py` rewrites each
`screenshots/...` reference in the markdown to an absolute path so pandoc
finds them regardless of its working directory, and `header.tex`'s
`\floatplacement{figure}{H}` keeps each image pinned immediately after its
paragraph instead of LaTeX floating it elsewhere on the page.

The results for the dashboard, alerts, and correlated-logs points (originally
slots `07`–`09`) are covered in prose only, without a dedicated screenshot —
add one back the same way as the others if wanted: reference
`screenshots/NN-name.png` in the markdown at that point, add an entry to
`gen_placeholders.py`'s `SLOTS` list, drop the real file in, rebuild.

| File | Capture |
|---|---|
| `01-compose-ps.png` | Terminal: `docker compose ... ps` — all 10 containers healthy |
| `02-prometheus-targets.png` | Prometheus `/targets` — all rows `UP` |
| `03-grafana-live-timeseries.png` | Grafana's live load/solar-by-zone panel |
| `04-airflow-dag-graph.png` | Airflow Graph tab — all 6 tasks green |
| `05-billing-api-final.png` | Swagger UI — `GET /api/billing/daily/{date}`, `"status": "final"` visible |
| `06-billing-report-html.png` | The rendered `reports/published/billing_report_<date>.html` |

To regenerate a placeholder (e.g. reset one back after a bad capture):
`python3 gen_placeholders.py --force` (omit `--force` to only fill slots
that don't exist yet, e.g. on a fresh checkout).

## Known constraint on the Markdown source

Do not let an inline code span (single backticks) wrap across a source line
in `EC8203_Report_DRAFT.md` — CommonMark turns the embedded newline into a
literal space inside the span (e.g. `` `energy_common.domain.transforms.\nenergy_balance` ``
renders as `energy_common.domain.transforms. energy_balance`, with a stray
space). Keep each inline code span on one source line, however long.

## Regenerating after edits

Re-run the two commands above after editing `EC8203_Report_DRAFT.md`; only
re-render a diagram (via mermaid-cli) if you changed one of the `.mmd` files.
