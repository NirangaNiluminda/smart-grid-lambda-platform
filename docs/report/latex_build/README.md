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
        "-V", "author=G.W. Niranga Niluminda (Dev A) \\\\and Dev B",
        "-V", "date=EC8203 Applied Big Data Engineering --- Mini Project Report",
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

## Known constraint on the Markdown source

Do not let an inline code span (single backticks) wrap across a source line
in `EC8203_Report_DRAFT.md` — CommonMark turns the embedded newline into a
literal space inside the span (e.g. `` `energy_common.domain.transforms.\nenergy_balance` ``
renders as `energy_common.domain.transforms. energy_balance`, with a stray
space). Keep each inline code span on one source line, however long.

## Regenerating after edits

Re-run the two commands above after editing `EC8203_Report_DRAFT.md`; only
re-render a diagram (via mermaid-cli) if you changed one of the `.mmd` files.
