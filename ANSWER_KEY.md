# Answer Key — Pipeline Bug Hunt

> **Do not read this file until you have finished your own analysis.**

---

## Bug 1: Mutable Default Argument (Mutable Data Structure)

**Location:** `SensorRegistry.register()` — line 55, parameter `tags=[]`

**Type:** Mutable default argument

**Severity:** Medium

In Python, default mutable arguments are evaluated once at function definition time
and shared across all subsequent calls. When `register()` is called without an
explicit `tags` argument, every such sensor receives a reference to the **same**
list object. Mutating one sensor's tags (e.g. `sensor.tags.append("maintenance")`)
silently mutates the tags for every other sensor that was registered without
explicit tags.

**Fix:** Use `tags: list = None` as the default and initialize inside the body:

```python
def register(self, ..., tags: list = None) -> SensorConfig:
    if tags is None:
        tags = []
    ...
```

---

## Bug 2: Off-by-One in Rolling Average Window (Logic Bug)

**Location:** `ProcessingPipeline._rolling_average()` — line 155, the `start` index calculation

**Type:** Logic error (off-by-one)

**Severity:** Medium

The window start is computed as `max(0, i - self._window_size)` but should be
`max(0, i - self._window_size + 1)`. Once the window reaches full size, it
contains `window_size + 1` elements instead of `window_size`. Because the code
divides by `len(window)`, the averages are slightly diluted by one extra stale
data point. The error is small and only manifests once the number of readings
exceeds `window_size`, making it easy to miss in casual testing.

**Example** (window_size=5, 8 readings for temp-001):
- At index 5: buggy window has 6 elements, correct window has 5
- Buggy avg ≈ 183.02, correct avg ≈ 183.52

**Fix:**

```python
start = max(0, i - self._window_size + 1)
```

---

## Bug 3: Unhandled Empty List After Outlier Removal (Edge Case)

**Location:** `ProcessingPipeline.process()` → `_compute_stats()` — lines 139–140

**Type:** Missing guard / edge case

**Severity:** High

If `_remove_outliers()` filters out **all** readings for a sensor (e.g. every
value falls outside the configured thresholds), the `cleaned` list is empty.
The `process()` method checks `if not readings` for the raw input but never
checks whether `cleaned` is empty after outlier removal. The empty list is then
passed to `_compute_stats()`, which calls `statistics.mean([])` — raising
`statistics.StatisticsError`.

**Fix:** Add a guard after outlier removal:

```python
cleaned = self._remove_outliers(sensor_id, sorted_readings)
if not cleaned:
    continue
```

---

## Bug 4: Truthiness Check Conflates `[]` with `None` (Subtle Conditional)

**Location:** `PipelineOrchestrator.register_sensor()` — line 234, `if tags:`

**Type:** Truthiness / identity confusion

**Severity:** Low–Medium

The method uses `if tags:` to decide whether to forward tags to the registry.
In Python, an empty list `[]` is **falsy**, so explicitly passing `tags=[]`
(meaning "this sensor intentionally has no tags") takes the same branch as
`tags=None` (meaning "no tags argument provided"). This causes the call to
fall through to `registry.register()` without the `tags` argument, triggering
the mutable default argument from Bug 1.

**Fix:**

```python
if tags is not None:
    return self.registry.register(...)
```

---

## Bug 5: Division by Zero in Anomaly Detection (Input-Specific)

**Location:** `ProcessingPipeline._detect_anomalies()` — line 175

**Type:** Division by zero / missing guard

**Severity:** High

The rate of change is calculated as:

```python
dt = (curr.timestamp - prev.timestamp).total_seconds()
rate_of_change = abs(curr.value - prev.value) / dt
```

If two consecutive readings share the same timestamp, `dt` is `0` and this
raises `ZeroDivisionError`. This occurs when duplicate or corrected records
are ingested with identical timestamps for the same sensor — the pipeline
has no deduplication step, so both readings survive to the anomaly detector.

**Fix:**

```python
if dt == 0:
    continue
```

---

## Summary Table

| # | Type                    | Location                            | Trigger Condition                         |
|---|-------------------------|-------------------------------------|-------------------------------------------|
| 1 | Mutable default arg     | `SensorRegistry.register()`         | Register 2+ sensors without explicit tags |
| 2 | Logic (off-by-one)      | `_rolling_average()`                | Sensor with > `window_size` readings      |
| 3 | Edge case               | `_compute_stats()` via `process()`  | All readings outside threshold range      |
| 4 | Subtle conditional      | `PipelineOrchestrator.register_sensor()` | Pass `tags=[]` explicitly            |
| 5 | Division by zero        | `_detect_anomalies()`               | Two readings with identical timestamps    |
