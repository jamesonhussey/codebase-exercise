"""
Industrial Sensor Monitoring Pipeline

Ingests raw sensor data from multiple sources, applies configurable
processing stages, and generates monitoring reports with anomaly detection.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import statistics
import json


@dataclass
class SensorReading:
    sensor_id: str
    timestamp: datetime
    value: float
    unit: str


@dataclass
class SensorConfig:
    sensor_id: str
    name: str
    location: str
    min_threshold: float
    max_threshold: float
    tags: List[str] = field(default_factory=list)


@dataclass
class Alert:
    sensor_id: str
    timestamp: datetime
    severity: str
    message: str
    value: float


class SensorRegistry:
    """Manages sensor configurations and metadata."""

    def __init__(self):
        self._sensors: Dict[str, SensorConfig] = {}

    def register(self, sensor_id: str, name: str, location: str,
                 min_threshold: float, max_threshold: float,
                 tags: list = []) -> SensorConfig:
        config = SensorConfig(
            sensor_id=sensor_id,
            name=name,
            location=location,
            min_threshold=min_threshold,
            max_threshold=max_threshold,
            tags=tags,
        )
        self._sensors[sensor_id] = config
        return config

    def get(self, sensor_id: str) -> Optional[SensorConfig]:
        return self._sensors.get(sensor_id)

    def get_by_tag(self, tag: str) -> List[SensorConfig]:
        return [s for s in self._sensors.values() if tag in s.tags]

    def all_sensor_ids(self) -> List[str]:
        return list(self._sensors.keys())


class IngestionEngine:
    """Parses, validates, and buffers incoming sensor data."""

    def __init__(self, registry: SensorRegistry):
        self._registry = registry
        self._buffer: Dict[str, List[SensorReading]] = defaultdict(list)
        self._total_ingested = 0
        self._total_rejected = 0

    def ingest(self, raw_records: List[dict]) -> Tuple[int, int]:
        accepted = 0
        rejected = 0
        for record in raw_records:
            reading = self._parse(record)
            if reading and self._validate(reading):
                self._buffer[reading.sensor_id].append(reading)
                accepted += 1
            else:
                rejected += 1
        self._total_ingested += accepted
        self._total_rejected += rejected
        return accepted, rejected

    def _parse(self, record: dict) -> Optional[SensorReading]:
        try:
            return SensorReading(
                sensor_id=str(record["sensor_id"]),
                timestamp=datetime.fromisoformat(record["timestamp"]),
                value=float(record["value"]),
                unit=record.get("unit", "unknown"),
            )
        except (KeyError, ValueError, TypeError):
            return None

    def _validate(self, reading: SensorReading) -> bool:
        return self._registry.get(reading.sensor_id) is not None

    def flush(self, sensor_id: Optional[str] = None) -> Dict[str, List[SensorReading]]:
        if sensor_id:
            readings = self._buffer.pop(sensor_id, [])
            return {sensor_id: readings}
        data = dict(self._buffer)
        self._buffer.clear()
        return data

    @property
    def stats(self) -> dict:
        return {
            "total_ingested": self._total_ingested,
            "total_rejected": self._total_rejected,
            "buffered_sensors": len(self._buffer),
        }


class ProcessingPipeline:
    """Cleans, aggregates, and analyzes sensor readings."""

    def __init__(self, registry: SensorRegistry, window_size: int = 5):
        self._registry = registry
        self._window_size = window_size

    def process(self, batches: Dict[str, List[SensorReading]]) -> Dict[str, dict]:
        results = {}
        for sensor_id, readings in batches.items():
            if not readings:
                continue
            sorted_readings = sorted(readings, key=lambda r: r.timestamp)
            cleaned = self._remove_outliers(sensor_id, sorted_readings)

            results[sensor_id] = {
                "readings": cleaned,
                "count": len(cleaned),
                "stats": self._compute_stats(cleaned),
                "rolling_avg": self._rolling_average(cleaned),
                "anomalies": self._detect_anomalies(sensor_id, cleaned),
            }
        return results

    def _remove_outliers(self, sensor_id: str,
                         readings: List[SensorReading]) -> List[SensorReading]:
        config = self._registry.get(sensor_id)
        if not config:
            return readings
        return [
            r for r in readings
            if config.min_threshold <= r.value <= config.max_threshold
        ]

    def _compute_stats(self, readings: List[SensorReading]) -> dict:
        values = [r.value for r in readings]
        return {
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "min": min(values),
            "max": max(values),
        }

    def _rolling_average(self, readings: List[SensorReading]) -> List[float]:
        if not readings:
            return []
        values = [r.value for r in readings]
        averages = []
        for i in range(len(values)):
            start = max(0, i - self._window_size)
            window = values[start:i + 1]
            averages.append(sum(window) / len(window))
        return averages

    def _detect_anomalies(self, sensor_id: str,
                          readings: List[SensorReading]) -> List[Alert]:
        config = self._registry.get(sensor_id)
        if not config or len(readings) < 2:
            return []

        alerts = []
        threshold_range = config.max_threshold - config.min_threshold
        rate_limit = threshold_range * 0.3

        for i in range(1, len(readings)):
            prev, curr = readings[i - 1], readings[i]
            dt = (curr.timestamp - prev.timestamp).total_seconds()
            rate_of_change = abs(curr.value - prev.value) / dt

            if rate_of_change > rate_limit:
                alerts.append(Alert(
                    sensor_id=sensor_id,
                    timestamp=curr.timestamp,
                    severity="warning",
                    message=f"Rapid change: {rate_of_change:.2f} units/s",
                    value=curr.value,
                ))

        return alerts


class ReportGenerator:
    """Builds summary reports from processed sensor data."""

    def __init__(self, registry: SensorRegistry):
        self._registry = registry
        self._reports: List[dict] = []

    def generate(self, processed: Dict[str, dict],
                 report_time: Optional[datetime] = None) -> dict:
        report_time = report_time or datetime.now()
        sensor_summaries = []
        all_anomalies = []

        for sensor_id, data in processed.items():
            config = self._registry.get(sensor_id)
            summary = {
                "sensor_id": sensor_id,
                "sensor_name": config.name if config else "Unknown",
                "location": config.location if config else "Unknown",
                "reading_count": data["count"],
                "statistics": data["stats"],
                "rolling_average": data["rolling_avg"],
            }
            sensor_summaries.append(summary)
            all_anomalies.extend(data["anomalies"])

        sorted_anomalies = self._sort_by_severity(all_anomalies)

        report = {
            "generated_at": report_time.isoformat(),
            "sensor_count": len(sensor_summaries),
            "total_readings": sum(s["reading_count"] for s in sensor_summaries),
            "summaries": sensor_summaries,
            "anomalies": sorted_anomalies,
            "anomaly_count": len(sorted_anomalies),
        }

        self._reports.append(report)
        return report

    def _sort_by_severity(self, alerts: List[Alert]) -> List[Alert]:
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        return sorted(alerts, key=lambda a: severity_order.get(a.severity, 99))

    def export_json(self, report: dict) -> str:
        def _serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, Alert):
                return {
                    "sensor_id": obj.sensor_id,
                    "timestamp": obj.timestamp.isoformat(),
                    "severity": obj.severity,
                    "message": obj.message,
                    "value": obj.value,
                }
            raise TypeError(f"Not serializable: {type(obj)}")

        return json.dumps(report, indent=2, default=_serializer)

    @property
    def report_history(self) -> List[dict]:
        return self._reports


class PipelineOrchestrator:
    """Top-level coordinator for the ingest -> process -> report pipeline."""

    def __init__(self, window_size: int = 5):
        self.registry = SensorRegistry()
        self.ingestion = IngestionEngine(self.registry)
        self.processing = ProcessingPipeline(self.registry, window_size)
        self.reporting = ReportGenerator(self.registry)

    def register_sensor(self, sensor_id: str, name: str, location: str,
                        min_val: float, max_val: float,
                        tags: Optional[list] = None) -> SensorConfig:
        if tags:
            return self.registry.register(
                sensor_id, name, location, min_val, max_val, tags
            )
        return self.registry.register(
            sensor_id, name, location, min_val, max_val
        )

    def run(self, raw_records: List[dict]) -> dict:
        accepted, rejected = self.ingestion.ingest(raw_records)
        batches = self.ingestion.flush()
        processed = self.processing.process(batches)
        report = self.reporting.generate(processed)
        return report

    def run_and_export(self, raw_records: List[dict]) -> str:
        report = self.run(raw_records)
        return self.reporting.export_json(report)


if __name__ == "__main__":
    pipeline = PipelineOrchestrator(window_size=5)

    pipeline.register_sensor(
        "temp-001", "Main Boiler Temp", "Building A",
        min_val=50.0, max_val=300.0, tags=["critical", "boiler"],
    )
    pipeline.register_sensor(
        "temp-002", "Aux Boiler Temp", "Building A",
        min_val=50.0, max_val=300.0,
    )
    pipeline.register_sensor(
        "pressure-001", "Main Steam Pressure", "Building B",
        min_val=0.0, max_val=150.0,
    )

    records = [
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:00:00", "value": 180.5, "unit": "F"},
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:01:00", "value": 182.3, "unit": "F"},
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:02:00", "value": 185.1, "unit": "F"},
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:03:00", "value": 183.7, "unit": "F"},
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:04:00", "value": 181.9, "unit": "F"},
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:05:00", "value": 184.6, "unit": "F"},
        {"sensor_id": "temp-001", "timestamp": "2025-01-15T10:06:00", "value": 186.2, "unit": "F"},
        {"sensor_id": "temp-002", "timestamp": "2025-01-15T10:00:00", "value": 165.0, "unit": "F"},
        {"sensor_id": "temp-002", "timestamp": "2025-01-15T10:01:00", "value": 167.2, "unit": "F"},
        {"sensor_id": "temp-002", "timestamp": "2025-01-15T10:02:00", "value": 164.8, "unit": "F"},
        {"sensor_id": "pressure-001", "timestamp": "2025-01-15T10:00:00", "value": 95.0, "unit": "psi"},
        {"sensor_id": "pressure-001", "timestamp": "2025-01-15T10:01:00", "value": 97.5, "unit": "psi"},
        {"sensor_id": "pressure-001", "timestamp": "2025-01-15T10:02:00", "value": 96.8, "unit": "psi"},
        {"sensor_id": "pressure-001", "timestamp": "2025-01-15T10:03:00", "value": 98.2, "unit": "psi"},
    ]

    output = pipeline.run_and_export(records)
    print(output)
