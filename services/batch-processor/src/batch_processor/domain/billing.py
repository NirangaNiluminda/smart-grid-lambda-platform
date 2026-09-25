"""Billing strategy domain implementations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol

from energy_common.domain.models import BillingTier, TariffRecord
from energy_common.domain.transforms import EnergyBalance


def _round_currency(val: Decimal) -> Decimal:
    return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True, slots=True)
class Charge:
    energy_charge: Decimal
    solar_credit: Decimal
    net_amount: Decimal


class BillingStrategy(Protocol):
    def calculate_charge(
        self,
        balance: EnergyBalance,
        tariff: TariffRecord,
        export_rate: Decimal,
    ) -> Charge: ...


class FlatBillingStrategy:
    """Flat rate per kWh of grid import minus export rate per kWh of solar export."""

    def calculate_charge(
        self,
        balance: EnergyBalance,
        tariff: TariffRecord,
        export_rate: Decimal,
    ) -> Charge:
        import_kwh = Decimal(str(round(balance.grid_import_kwh, 4)))
        export_kwh = Decimal(str(round(balance.solar_export_kwh, 4)))

        energy_charge = _round_currency(import_kwh * tariff.tariff_rate)
        solar_credit = _round_currency(export_kwh * export_rate)
        net_amount = energy_charge - solar_credit

        return Charge(
            energy_charge=energy_charge,
            solar_credit=solar_credit,
            net_amount=net_amount,
        )


class TieredBillingStrategy:
    """Tiered blocks with increasing multipliers per block."""

    def __init__(
        self,
        block_kwh: tuple[float, ...] = (5.0, 15.0),
        multipliers: tuple[float, ...] = (0.8, 1.0, 1.4),
    ) -> None:
        self.block_kwh = block_kwh
        self.multipliers = multipliers

    def calculate_charge(
        self,
        balance: EnergyBalance,
        tariff: TariffRecord,
        export_rate: Decimal,
    ) -> Charge:
        grid_import = balance.grid_import_kwh
        base_rate = tariff.tariff_rate

        b1_cap = self.block_kwh[0]
        b2_cap = self.block_kwh[1]

        kwh_b1 = min(grid_import, b1_cap)
        kwh_b2 = min(max(grid_import - b1_cap, 0.0), b2_cap - b1_cap)
        kwh_b3 = max(grid_import - b2_cap, 0.0)

        charge_b1 = Decimal(str(round(kwh_b1, 4))) * base_rate * Decimal(str(self.multipliers[0]))
        charge_b2 = Decimal(str(round(kwh_b2, 4))) * base_rate * Decimal(str(self.multipliers[1]))
        charge_b3 = Decimal(str(round(kwh_b3, 4))) * base_rate * Decimal(str(self.multipliers[2]))

        energy_charge = _round_currency(charge_b1 + charge_b2 + charge_b3)
        solar_credit = _round_currency(
            Decimal(str(round(balance.solar_export_kwh, 4))) * export_rate
        )
        net_amount = energy_charge - solar_credit

        return Charge(
            energy_charge=energy_charge,
            solar_credit=solar_credit,
            net_amount=net_amount,
        )


class SubsidisedBillingStrategy:
    """Subsidised rate: 50% discount on the first block."""

    def __init__(
        self,
        first_block_kwh: float = 5.0,
        discount_factor: float = 0.5,
    ) -> None:
        self.first_block_kwh = first_block_kwh
        self.discount_factor = discount_factor

    def calculate_charge(
        self,
        balance: EnergyBalance,
        tariff: TariffRecord,
        export_rate: Decimal,
    ) -> Charge:
        grid_import = balance.grid_import_kwh
        base_rate = tariff.tariff_rate

        kwh_subsidised = min(grid_import, self.first_block_kwh)
        kwh_standard = max(grid_import - self.first_block_kwh, 0.0)

        charge_sub = (
            Decimal(str(round(kwh_subsidised, 4))) * base_rate * Decimal(str(self.discount_factor))
        )
        charge_std = Decimal(str(round(kwh_standard, 4))) * base_rate

        energy_charge = _round_currency(charge_sub + charge_std)
        solar_credit = _round_currency(
            Decimal(str(round(balance.solar_export_kwh, 4))) * export_rate
        )
        net_amount = energy_charge - solar_credit

        return Charge(
            energy_charge=energy_charge,
            solar_credit=solar_credit,
            net_amount=net_amount,
        )


def get_billing_strategy(tier: BillingTier | str) -> BillingStrategy:
    tier_str = tier.value if isinstance(tier, BillingTier) else str(tier).lower()
    if tier_str == BillingTier.TIERED.value:
        return TieredBillingStrategy()
    if tier_str == BillingTier.SUBSIDISED.value:
        return SubsidisedBillingStrategy()
    return FlatBillingStrategy()
