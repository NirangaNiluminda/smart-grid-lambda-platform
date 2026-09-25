"""Artifact reporting adapter for batch billing."""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from energy_common.domain.models import HouseholdBill, ZoneSolarSummary
from jinja2 import Template

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Daily Energy Reconciliation & Billing Report — {{ business_date }}</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; margin: 2rem; color: #1e293b; }
        h1, h2 { color: #0f172a; }
        .summary-cards { display: flex; gap: 1rem; margin-bottom: 2rem; }
        .card {
            background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
            padding: 1rem 1.5rem; flex: 1;
        }
        .card .title { font-size: 0.875rem; color: #64748b; }
        .card .value { font-size: 1.5rem; font-weight: bold; margin-top: 0.5rem; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { border: 1px solid #e2e8f0; padding: 8px 12px; text-align: left; }
        th { background: #f1f5f9; }
        tr:nth-child(even) { background: #f8fafc; }
        .num { text-align: right; }
    </style>
</head>
<body>
    <h1>Smart Grid Billing & Solar Reconciliation</h1>
    <p>Business Date: <strong>{{ business_date }}</strong></p>

    <div class="summary-cards">
        <div class="card">
            <div class="title">Total Consumption</div>
            <div class="value">{{ "%.2f"|format(total_consumption) }} kWh</div>
        </div>
        <div class="card">
            <div class="title">Total Solar Generated</div>
            <div class="value">{{ "%.2f"|format(total_solar) }} kWh</div>
        </div>
        <div class="card">
            <div class="title">Total Billed Amount</div>
            <div class="value">LKR {{ "%.2f"|format(total_net) }}</div>
        </div>
        <div class="card">
            <div class="title">Households Billed</div>
            <div class="value">{{ bills|length }}</div>
        </div>
    </div>

    <h2>Per-Zone Solar Summary</h2>
    <table>
        <thead>
            <tr>
                <th>Zone</th>
                <th class="num">Consumption (kWh)</th>
                <th class="num">Solar Gen (kWh)</th>
                <th class="num">Renewable %</th>
                <th class="num">Households</th>
            </tr>
        </thead>
        <tbody>
            {% for s in summaries %}
            <tr>
                <td>{{ s.grid_zone }}</td>
                <td class="num">{{ "%.2f"|format(s.total_consumption_kwh) }}</td>
                <td class="num">{{ "%.2f"|format(s.total_solar_generation_kwh) }}</td>
                <td class="num">{{ "%.1f"|format(s.renewable_pct) }}%</td>
                <td class="num">{{ s.household_count }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>

    <h2>Household Billing Details</h2>
    <table>
        <thead>
            <tr>
                <th>Household</th>
                <th>Zone</th>
                <th>Tier</th>
                <th class="num">Grid Import (kWh)</th>
                <th class="num">Solar Export (kWh)</th>
                <th class="num">Energy Charge</th>
                <th class="num">Solar Credit</th>
                <th class="num">Net Amount</th>
            </tr>
        </thead>
        <tbody>
            {% for b in bills %}
            <tr>
                <td>{{ b.household_id }}</td>
                <td>{{ b.grid_zone }}</td>
                <td>{{ b.billing_tier.value }}{% if b.subsidy_flag %} (subsidised){% endif %}</td>
                <td class="num">{{ "%.2f"|format(b.grid_import_kwh) }}</td>
                <td class="num">{{ "%.2f"|format(b.solar_export_kwh) }}</td>
                <td class="num">{{ "%.2f"|format(b.energy_charge) }}</td>
                <td class="num">{{ "%.2f"|format(b.solar_credit) }}</td>
                <td class="num"><strong>{{ "%.2f"|format(b.net_amount) }}</strong></td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""


class ReportRenderer:
    """Renders CSV and HTML report artifacts."""

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir

    def render(
        self,
        business_date: dt.date,
        bills: list[HouseholdBill],
        summaries: list[ZoneSolarSummary],
    ) -> tuple[Path, Path]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = self.out_dir / f"billing_report_{business_date}.csv"
        html_path = self.out_dir / f"billing_report_{business_date}.html"

        # 1. Write CSV
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "business_date",
                    "household_id",
                    "grid_zone",
                    "total_consumption_kwh",
                    "total_solar_kwh",
                    "self_consumed_kwh",
                    "grid_import_kwh",
                    "solar_export_kwh",
                    "tariff_rate",
                    "billing_tier",
                    "subsidy_flag",
                    "energy_charge",
                    "solar_credit",
                    "net_amount",
                    "currency",
                ]
            )
            for b in bills:
                writer.writerow(
                    [
                        b.business_date.isoformat(),
                        b.household_id,
                        b.grid_zone,
                        b.total_consumption_kwh,
                        b.total_solar_kwh,
                        b.self_consumed_kwh,
                        b.grid_import_kwh,
                        b.solar_export_kwh,
                        str(b.tariff_rate),
                        b.billing_tier.value,
                        b.subsidy_flag,
                        str(b.energy_charge),
                        str(b.solar_credit),
                        str(b.net_amount),
                        b.currency,
                    ]
                )

        # 2. Write HTML
        template = Template(HTML_TEMPLATE)
        total_consumption = sum(b.total_consumption_kwh for b in bills)
        total_solar = sum(b.total_solar_kwh for b in bills)
        total_net = sum(b.net_amount for b in bills)

        rendered_html = template.render(
            business_date=business_date.isoformat(),
            bills=bills,
            summaries=summaries,
            total_consumption=total_consumption,
            total_solar=total_solar,
            total_net=total_net,
        )

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(rendered_html)

        return csv_path, html_path
