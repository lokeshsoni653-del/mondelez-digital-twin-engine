"""
Mondelēz Hub Plant — IoT Telemetry Pipeline
Cadbury_Dairy_Milk_Main Production Line Sensor Simulator

Tracks:
  - cocoa_inventory_kg      : primary ingredient, depletion-critical
  - sugar_inventory_kg      : secondary ingredient
  - conveyor_speed_mpm      : metres per minute
  - temperature_c           : processing temperature (°C)
  - line_efficiency_pct     : derived KPI
  - throughput_units_hr     : units produced per hour (derived)

Alert thresholds are defined in config/thresholds.json and loaded at startup.
"""

import time
import math
import random
import json
import logging
import threading
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, asdict, field
from enum import Enum

logger = logging.getLogger("telemetry_pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# Domain Models
# ─────────────────────────────────────────────────────────────────────────────

class AlertLevel(str, Enum):
    OK      = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertCode(str, Enum):
    COCOA_CRITICAL_DEPLETION   = "critical_depletion_alert"
    COCOA_LOW                  = "cocoa_low_warning"
    SUGAR_LOW                  = "sugar_low_warning"
    TEMP_OVERHEAT              = "temperature_overheat"
    TEMP_UNDERHEAT             = "temperature_underheat"
    CONVEYOR_SLOW              = "conveyor_speed_low"
    CONVEYOR_FAST              = "conveyor_speed_high"


@dataclass
class Alert:
    code: str
    level: str
    message: str
    sensor: str
    value: float
    threshold: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class SensorReading:
    """Single telemetry snapshot from the CDM production line."""
    timestamp: float
    line_id: str
    cocoa_inventory_kg: float
    sugar_inventory_kg: float
    conveyor_speed_mpm: float
    temperature_c: float
    line_efficiency_pct: float
    throughput_units_hr: int
    alerts: List[Dict[str, Any]]
    sequence: int


# ─────────────────────────────────────────────────────────────────────────────
# Default Thresholds (also persisted to config/thresholds.json)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "cocoa_critical_kg":     800.0,
    "cocoa_warning_kg":     1200.0,
    "sugar_warning_kg":      600.0,
    "temp_max_c":             42.0,
    "temp_min_c":             28.0,
    "conveyor_min_mpm":       12.0,
    "conveyor_max_mpm":       38.0,
}


# ─────────────────────────────────────────────────────────────────────────────
# Sensor State Machine
# ─────────────────────────────────────────────────────────────────────────────

class ProductionLineSimulator:
    """
    Stateful simulator for the Cadbury_Dairy_Milk_Main line.

    Uses sinusoidal drift + noise to mimic real sensor behaviour.
    Cocoa inventory follows a slow depletion curve with occasional
    partial restocks so the critical alert fires and recovers.
    """

    LINE_ID = "Cadbury_Dairy_Milk_Main"

    # Initial inventory levels
    COCOA_INIT   = 2500.0   # kg
    SUGAR_INIT   = 1800.0   # kg

    # Consumption rates per tick (at 1-second intervals)
    COCOA_DRAIN_BASE  = 1.4   # kg/s  (before noise)
    SUGAR_DRAIN_BASE  = 0.9   # kg/s

    # Conveyor nominal
    CONVEYOR_NOMINAL = 24.0   # mpm

    # Temperature nominal
    TEMP_NOMINAL     = 34.0   # °C

    def __init__(self, thresholds: Dict[str, float]):
        self.thresholds = thresholds
        self._seq = 0

        # Inventory state
        self._cocoa  = self.COCOA_INIT
        self._sugar  = self.SUGAR_INIT

        # Phase offsets for sinusoidal variation
        self._phase_temp      = random.uniform(0, 2 * math.pi)
        self._phase_conveyor  = random.uniform(0, 2 * math.pi)
        self._tick = 0

        # Restock scheduling
        self._restock_pending = False
        self._restock_at_tick = self._next_restock_tick()

    def _next_restock_tick(self) -> int:
        """Schedule a restock between 600-900 ticks (~10-15 min at 1Hz)."""
        return self._tick + random.randint(600, 900)

    def tick(self) -> SensorReading:
        """Advance simulation by one step and return a SensorReading."""
        self._tick += 1
        self._seq  += 1

        # ── Inventory depletion ──────────────────────────────────────────────
        cocoa_noise = random.gauss(0, 0.15)
        sugar_noise = random.gauss(0, 0.10)
        self._cocoa = max(0.0, self._cocoa - (self.COCOA_DRAIN_BASE + cocoa_noise))
        self._sugar = max(0.0, self._sugar - (self.SUGAR_DRAIN_BASE + sugar_noise))

        # Restock trigger — partial refill keeps behaviour interesting
        if self._tick >= self._restock_at_tick:
            refill_cocoa = random.uniform(1200, 1600)
            refill_sugar = random.uniform(700,  1000)
            self._cocoa = min(self.COCOA_INIT, self._cocoa + refill_cocoa)
            self._sugar = min(self.SUGAR_INIT, self._sugar + refill_sugar)
            self._restock_at_tick = self._next_restock_tick()
            logger.info(
                f"Restock event — cocoa+{refill_cocoa:.0f}kg  sugar+{refill_sugar:.0f}kg"
            )

        # ── Conveyor speed ───────────────────────────────────────────────────
        conveyor = (
            self.CONVEYOR_NOMINAL
            + 5.0 * math.sin(self._tick * 0.015 + self._phase_conveyor)
            + random.gauss(0, 0.8)
        )
        conveyor = round(max(0.0, conveyor), 2)

        # ── Temperature ─────────────────────────────────────────────────────
        temperature = (
            self.TEMP_NOMINAL
            + 4.5 * math.sin(self._tick * 0.008 + self._phase_temp)
            + random.gauss(0, 0.5)
        )
        temperature = round(temperature, 2)

        # ── Derived KPIs ─────────────────────────────────────────────────────
        speed_factor = min(1.0, conveyor / self.CONVEYOR_NOMINAL)
        efficiency   = round(speed_factor * random.uniform(88, 99), 1)
        throughput   = int(efficiency * 1.4)   # units/hr proportional to efficiency

        # ── Alert evaluation ─────────────────────────────────────────────────
        alerts = self._evaluate_alerts(
            cocoa=self._cocoa,
            sugar=self._sugar,
            temp=temperature,
            conveyor=conveyor,
        )

        return SensorReading(
            timestamp           = time.time(),
            line_id             = self.LINE_ID,
            cocoa_inventory_kg  = round(self._cocoa, 2),
            sugar_inventory_kg  = round(self._sugar, 2),
            conveyor_speed_mpm  = conveyor,
            temperature_c       = temperature,
            line_efficiency_pct = efficiency,
            throughput_units_hr = throughput,
            alerts              = [asdict(a) for a in alerts],
            sequence            = self._seq,
        )

    def _evaluate_alerts(
        self,
        cocoa: float,
        sugar: float,
        temp: float,
        conveyor: float,
    ) -> List[Alert]:
        """
        Evaluate all sensor values against thresholds.
        Returns a list of active Alert objects.
        """
        alerts: List[Alert] = []
        t = self.thresholds

        # ── Cocoa critical depletion (primary business rule) ─────────────────
        if cocoa < t["cocoa_critical_kg"]:
            alerts.append(Alert(
                code      = AlertCode.COCOA_CRITICAL_DEPLETION.value,
                level     = AlertLevel.CRITICAL.value,
                message   = (
                    f"⚠ CRITICAL: Cocoa inventory at {cocoa:.1f}kg — "
                    f"below critical threshold of {t['cocoa_critical_kg']:.0f}kg. "
                    "Halt or reduce line speed immediately."
                ),
                sensor    = "cocoa_inventory_kg",
                value     = cocoa,
                threshold = t["cocoa_critical_kg"],
            ))
        elif cocoa < t["cocoa_warning_kg"]:
            alerts.append(Alert(
                code      = AlertCode.COCOA_LOW.value,
                level     = AlertLevel.WARNING.value,
                message   = (
                    f"Cocoa inventory low: {cocoa:.1f}kg "
                    f"(warning threshold: {t['cocoa_warning_kg']:.0f}kg). "
                    "Schedule restock."
                ),
                sensor    = "cocoa_inventory_kg",
                value     = cocoa,
                threshold = t["cocoa_warning_kg"],
            ))

        # ── Sugar warning ────────────────────────────────────────────────────
        if sugar < t["sugar_warning_kg"]:
            alerts.append(Alert(
                code      = AlertCode.SUGAR_LOW.value,
                level     = AlertLevel.WARNING.value,
                message   = (
                    f"Sugar inventory low: {sugar:.1f}kg "
                    f"(threshold: {t['sugar_warning_kg']:.0f}kg)."
                ),
                sensor    = "sugar_inventory_kg",
                value     = sugar,
                threshold = t["sugar_warning_kg"],
            ))

        # ── Temperature bounds ───────────────────────────────────────────────
        if temp > t["temp_max_c"]:
            alerts.append(Alert(
                code      = AlertCode.TEMP_OVERHEAT.value,
                level     = AlertLevel.CRITICAL.value,
                message   = (
                    f"Temperature overheat: {temp:.1f}°C > {t['temp_max_c']}°C. "
                    "Check cooling system."
                ),
                sensor    = "temperature_c",
                value     = temp,
                threshold = t["temp_max_c"],
            ))
        elif temp < t["temp_min_c"]:
            alerts.append(Alert(
                code      = AlertCode.TEMP_UNDERHEAT.value,
                level     = AlertLevel.WARNING.value,
                message   = (
                    f"Temperature below minimum: {temp:.1f}°C < {t['temp_min_c']}°C."
                ),
                sensor    = "temperature_c",
                value     = temp,
                threshold = t["temp_min_c"],
            ))

        # ── Conveyor speed bounds ────────────────────────────────────────────
        if conveyor < t["conveyor_min_mpm"]:
            alerts.append(Alert(
                code      = AlertCode.CONVEYOR_SLOW.value,
                level     = AlertLevel.WARNING.value,
                message   = (
                    f"Conveyor speed low: {conveyor:.1f} mpm "
                    f"(min: {t['conveyor_min_mpm']} mpm)."
                ),
                sensor    = "conveyor_speed_mpm",
                value     = conveyor,
                threshold = t["conveyor_min_mpm"],
            ))
        elif conveyor > t["conveyor_max_mpm"]:
            alerts.append(Alert(
                code      = AlertCode.CONVEYOR_FAST.value,
                level     = AlertLevel.WARNING.value,
                message   = (
                    f"Conveyor overspeed: {conveyor:.1f} mpm "
                    f"(max: {t['conveyor_max_mpm']} mpm)."
                ),
                sensor    = "conveyor_speed_mpm",
                value     = conveyor,
                threshold = t["conveyor_max_mpm"],
            ))

        return alerts


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class TelemetryPipeline:
    """
    Drives the ProductionLineSimulator at a configurable tick rate and
    invokes registered broadcast callbacks with JSON-serialised payloads.
    """

    def __init__(
        self,
        thresholds: Optional[Dict[str, float]] = None,
        tick_rate_hz: float = 1.0,
    ):
        self.thresholds   = thresholds or DEFAULT_THRESHOLDS
        self.tick_rate_hz = tick_rate_hz
        self._interval    = 1.0 / tick_rate_hz
        self._simulator   = ProductionLineSimulator(self.thresholds)
        self._callbacks: List[Callable[[str], None]] = []
        self._running     = False
        self._thread: Optional[threading.Thread] = None

    def register_callback(self, cb: Callable[[str], None]):
        """Register a function that receives JSON-serialised SensorReading strings."""
        self._callbacks.append(cb)

    def start(self):
        """Start the pipeline in a daemon thread."""
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info(
            f"Telemetry pipeline started at {self.tick_rate_hz} Hz "
            f"(interval {self._interval*1000:.0f}ms)"
        )

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            start = time.perf_counter()
            try:
                reading = self._simulator.tick()
                payload  = json.dumps(asdict(reading))
                for cb in self._callbacks:
                    try:
                        cb(payload)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
            except Exception as e:
                logger.error(f"Simulator error: {e}")

            elapsed = time.perf_counter() - start
            sleep_for = max(0.0, self._interval - elapsed)
            time.sleep(sleep_for)
