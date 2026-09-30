"""Regenerates placeholder images in ../screenshots/ (only the ones that
don't already exist, unless --force is passed). Run once to bootstrap a
fresh checkout before the real screenshots are captured, or with --force
to reset a slot back to its placeholder. Requires Pillow (`pip install pillow`).
"""

import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
OUT_DIR = HERE.parent / "screenshots"

SLOTS = [
    ("01-compose-ps.png", "docker compose ... ps", "all 10 containers healthy"),
    ("02-prometheus-targets.png", "Prometheus /targets", "every target UP"),
    ("03-grafana-live-timeseries.png", "Grafana: live load/solar by zone", "top panel, auto-refreshing"),
    ("04-airflow-dag-graph.png", "Airflow: Graph tab", "6 tasks green"),
    ("05-billing-api-final.png", "Swagger UI: GET /api/billing/daily/{date}", '"status": "final"'),
    ("06-billing-report-html.png", "reports/published/billing_report_<date>.html", "rendered report"),
]

W, H = 1600, 900
FORCE = "--force" in sys.argv

try:
    font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 44)
    font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 30)
    font_tiny = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf", 24)
except OSError:
    font_big = font_small = font_tiny = ImageFont.load_default()

OUT_DIR.mkdir(parents=True, exist_ok=True)

for filename, title, subtitle in SLOTS:
    dest = OUT_DIR / filename
    if dest.exists() and not FORCE:
        print("skip (exists):", filename)
        continue

    img = Image.new("RGB", (W, H), color=(235, 238, 242))
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W - 1, H - 1], outline=(150, 160, 170), width=6)
    for x, y in [(20, 20), (W - 20, 20), (20, H - 20), (W - 20, H - 20)]:
        d.line([(x - 25, y), (x + 25, y)], fill=(150, 160, 170), width=4)
        d.line([(x, y - 25), (x, y + 25)], fill=(150, 160, 170), width=4)

    d.text((W / 2, H / 2 - 90), "PLACEHOLDER", font=font_big, fill=(120, 130, 145), anchor="mm")
    d.text((W / 2, H / 2 - 20), title, font=font_small, fill=(60, 70, 85), anchor="mm")
    d.text((W / 2, H / 2 + 30), subtitle, font=font_tiny, fill=(90, 100, 115), anchor="mm")
    d.text((W / 2, H / 2 + 100), f"replace this file: {filename}", font=font_tiny, fill=(150, 90, 90), anchor="mm")

    img.save(dest)
    print("wrote", filename)
