---
focus: arch
last_mapped: 2026-06-13
---
# Architecture

**Analysis Date:** 2026-06-13

## System Overview

```text
┌──────────────────────────────────────────────────────────────────────┐
│                        SwiftUI Views (177 .swift files)              │
│   RootView → AppShellView → [HomeView / HealthView / CoachView /    │
│                               MoreView]                              │
│   All views observe GooseAppModel via @Environment                  │
│   Health tab observes HealthDataStore (@State in AppShellView)      │
└────────────────────────┬───────────────────────────────┬────────────┘
                         │ @Observable                   │ @Observable
                         ▼                               ▼
┌───────────────────────────────┐     ┌─────────────────────────────┐
│  GooseAppModel (@MainActor)   │     │  HealthDataStore (@MainActor)│
│  GooseSwift/GooseAppModel.swift│    │  GooseSwift/HealthDataStore. │
│  + GooseAppModel+*.swift (9)  │     │  swift + HealthDataStore+*.  │
│                               │     │  swift (18 extensions)       │
│  Owns: BLE, pipelines,        │     │  Owns: own GooseRustBridge,  │
│  upload, strain accumulator   │     │  metric query state          │
└──────────┬────────────────────┘     └──────────┬──────────────────┘
           │ delegates raw bytes                  │ bridge.request(method:)
           ▼                                      ▼
┌───────────────────────────┐     ┌────────────────────────────────┐
│  CoreBluetoothBLETransport           │     │  GooseRustBridge               │
│  @Observable NSObject     │     │  GooseSwift/GooseRustBridge.    │
│  @unchecked Sendable      │     │  swift                         │
│  GooseSwift/CoreBluetoothBLETransport│     │  JSON-over-C-FFI wrapper       │
│  .swift + 11 extensions   │     │  Multiple instances per design  │
└──────────┬────────────────┘     └──────────┬──────────────────────┘
           │ CoreBluetooth callbacks          │ goose_bridge_handle_json()
           │                                  ▼
           │                    ┌──────────────────────────────────┐
           │                    │  Rust core (libgoose_core.a)     │
           │                    │  Rust/core/src/bridge.rs         │
           │                    │  76 .rs modules, schema v20      │
           │                    │  Protocol parsing, metric algos, │
           │                    │  SQLite persistence (rusqlite)   │
           └──────────────────►│  goose.sqlite                    │
           (via CaptureFrameWriteQueue       └──────────────────────────────────┘
            + OvernightSQLiteMirrorQueue)
```

## Component Responsibilities

| Component | Responsibility | File(s) |
|-----------|----------------|---------|
| `GooseSwiftApp` | App entry point; scene config; `@State` creation | `GooseSwift/GooseSwiftApp.swift` |
| `AppRouter` | Tab selection, deep-link handling, per-tab `NavigationPath` | `GooseSwift/AppRouter.swift` |
| `RootView` | Onboarding gate; renders `OnboardingView` or `AppShellView` | `GooseSwift/RootView.swift` |
| `AppShellView` | `TabView` wiring; creates `HealthDataStore`; injects into environment | `GooseSwift/AppShellView.swift` |
| `GooseAppModel` | Central `@MainActor @Observable` coordinator; owns all subsystems; 9 extension files | `GooseSwift/GooseAppModel.swift` + `GooseAppModel+*.swift` |
| `CoreBluetoothBLETransport` | `CBCentralManagerDelegate` + `CBPeripheralDelegate`; WHOOP GATT; command writes | `GooseSwift/CoreBluetoothBLETransport.swift` + 11 extensions |
| `GooseRustBridge` | JSON-over-FFI wrapper; calls `goose_bridge_handle_json` / `goose_bridge_free_string` | `GooseSwift/GooseRustBridge.swift` |
| `HealthDataStore` | `@MainActor @Observable`; metric queries via own bridge instance; 18 extensions | `GooseSwift/HealthDataStore.swift` + extensions |
| `NotificationFrameParser` | Delegates raw BLE bytes to Rust for frame reassembly | `GooseSwift/NotificationFrameParsing.swift` |
| `CaptureFrameWriteQueue` | Batched SQLite inserts of captured BLE frames | `GooseSwift/CaptureFrameWriteQueue.swift` |
| `OvernightSQLiteMirrorQueue` | Queues raw notification rows → Rust bridge insert | `GooseSwift/OvernightSQLiteMirrorQueue.swift` |
| `GooseStrainAccumulator` | Swift `actor`; accumulates live workout strain from HR samples | `GooseSwift/GooseStrainAccumulator.swift` |
| `GooseUploadService` | URLSession-based upload to self-hosted FastAPI server; per-sensor watermark | `GooseSwift/GooseUploadService.swift` |
| `GooseNetworkMonitor` | Network reachability; gates upload triggers | `GooseSwift/GooseNetworkMonitor.swift` |
| `PassiveActivityDetectionPipeline` | Heuristic HR/motion analysis; `.finished(summary, reason:)` triggers workout events | `GooseSwift/PassiveActivityDetector.swift` |
| `WorkoutLiveActivityController` | `ActivityKit` Live Activity lifecycle for workouts | `GooseSwift/WorkoutLiveActivityController.swift` |
| `GooseWorkoutLiveActivityWidget` | WidgetKit extension; Dynamic Island + lock-screen rendering | `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift` |
| Rust core | Protocol parsing, SQLite schema v20, metric algorithms, BLE frame import, 58+ bridge methods | `Rust/core/src/bridge.rs` (10 847 lines) |

## Pattern Overview

**Overall:** `@MainActor @Observable` coordinator + background `DispatchQueue` fan-out + synchronous Rust FFI

**Key Characteristics:**
- `GooseAppModel` is the single `@MainActor @Observable` coordinator; all UI mutation returns to it via `DispatchQueue.main.async { }` or `Task { @MainActor in ... }`.
- `GooseRustBridge` is **not** a singleton. `GooseAppModel`, `HealthDataStore`, `OvernightSQLiteMirrorQueue`, and `CaptureFrameWriteQueue` each hold their own instance. The Rust side is stateless across calls; SQLite is the shared persistent store.
- The Swift `@Observable` macro (iOS 17+, not the older `ObservableObject`) is used on `GooseAppModel` and `CoreBluetoothBLETransport`. `HealthDataStore` also uses `@Observable`. `AppRouter` uses the older `ObservableObject` / `@Published`.
- `GooseStrainAccumulator` is the only Swift `actor` in the codebase. All other concurrency uses `@MainActor` + named `DispatchQueue`.

## Layers

**UI Layer:**
- Purpose: SwiftUI rendering, user interaction
- Location: `GooseSwift/` — all `*View.swift`, `*Views.swift`, `*Screen.swift`
- Contains: SwiftUI `View` structs; view-local `@State`; no business logic
- Depends on: `GooseAppModel` and `HealthDataStore` via `@Environment`
- Used by: `AppShellView` tab builder

**Coordinator Layer:**
- Purpose: Business logic, BLE pipeline wiring, state machine, upload orchestration
- Location: `GooseSwift/GooseAppModel.swift` + `GooseAppModel+*.swift` (9 extension files)
- Contains: `@MainActor @Observable final class GooseAppModel`; all named dispatch queues; pipeline objects as `let` properties
- Depends on: `CoreBluetoothBLETransport`, `GooseRustBridge` (`let rust`), `CaptureFrameWriteQueue`, `GooseUploadService`, `GooseNetworkMonitor`
- Used by: `GooseSwiftApp`, all SwiftUI views

**Metric Query Layer:**
- Purpose: Query Rust bridge for scored metrics; publish results to Health tab views
- Location: `GooseSwift/HealthDataStore.swift` + `HealthDataStore+*.swift` (18 extensions)
- Contains: `@MainActor @Observable final class HealthDataStore`; `let bridge = GooseRustBridge()`; display-safe metric cache
- Depends on: its own `GooseRustBridge` instance; `HealthDataStore.defaultDatabasePath()`
- Used by: `AppShellView` (creates the one instance), all Health tab sub-views

**BLE Layer:**
- Purpose: CoreBluetooth central manager; WHOOP GATT protocol; command writes; notification callbacks
- Location: `GooseSwift/CoreBluetoothBLETransport.swift` + 11 extension files
- Contains: `CBCentralManagerDelegate`, `CBPeripheralDelegate`; frame command builders; haptic commands
- Depends on: CoreBluetooth, OSLog; calls back into `GooseAppModel` via closures/delegates
- Used by: `GooseAppModel` (holds the instance as `let ble`)

**FFI Bridge Layer:**
- Purpose: Type-safe JSON envelope over two C symbols
- Location: `GooseSwift/GooseRustBridge.swift`, `GooseSwift/GooseSwift-Bridging-Header.h`
- Contains: `final class GooseRustBridge: @unchecked Sendable`; `NSLock`-guarded counter and timing
- Depends on: `Rust/core/include/goose_core_bridge.h` — `goose_bridge_handle_json`, `goose_bridge_free_string`
- Testability protocol: `GooseRustBridging` at `GooseSwift/GooseRustBridging.swift`

**Rust Core:**
- Purpose: Protocol parsing, SQLite schema v20, metric algorithms, BLE frame import
- Location: `Rust/core/src/` (76 `.rs` files)
- Entry point: `Rust/core/src/bridge.rs` — receives JSON `method` string, dispatches to internal modules
- Depends on: `rusqlite 0.37` (bundled), `serde_json 1.0`, `serde 1.0`
- Used by: Swift via the C FFI pair only

**Upload Layer:**
- Purpose: Self-hosted server upload via native URLSession; no third-party dependencies
- Location: `GooseSwift/GooseUploadService.swift` + `GooseAppModel+Upload.swift`
- Contains: `GooseUploadService`; per-sensor upload watermark; APNs token integration
- Depends on: `GooseNetworkMonitor`, `GooseRustBridge` for pending-count queries

## Data Flow

### Primary Real-Time BLE → SQLite Path

1. WHOOP sends GATT notification → `CBPeripheralDelegate.peripheral(_:didUpdateValueFor:)` in `CoreBluetoothBLETransport+PeripheralDelegate.swift`
2. `GooseAppModel.handleNotification(_:)` called on `@MainActor` — `GooseAppModel+NotificationPipeline.swift`
3. Dispatched to `notificationIngestQueue` (background, `.utility`); `NotificationFrameParser` reassembles multi-packet frames via Rust
4. Result returned to `@MainActor`; if a capture session is active, `CaptureFrameWriteQueue.enqueue(_:)` batches rows
5. `CaptureFrameWriteQueue` coalesces writes → `GooseRustBridge.request(method: "capture.import_batch")` → `goose_bridge_handle_json()` → SQLite insert in Rust

### Metric Score Path (on-demand)

1. `HealthDataStore` method called (e.g., `loadRecoveryScores()`) on `@MainActor`
2. `Task.detached(priority: .userInitiated)` dispatched — never blocks main thread
3. `bridge.request(method: "metrics.run_packet_inputs", args: ["database_path": ...])` → Rust reads SQLite, computes scores
4. Result decoded on detached task → `await MainActor.run { ... }` updates `@Observable` state
5. SwiftUI body re-runs automatically

### Sleep Sync Path

1. BLE connects → `maybeScheduleMorningSleepSync()` in `GooseAppModel+SleepSync.swift`
2. `syncBandSleepHistory()` issues historical sync BLE commands via `CoreBluetoothBLETransport+HistoricalCommands.swift`
3. On completion, `onHistoricalSyncCompleted?()` callback fires (closure set by `AppShellView`) → triggers `HealthDataStore` metric refresh

### Upload Path

1. `GooseNetworkMonitor` reports reachability change → `GooseAppModel` calls `GooseUploadService`
2. Service reads pending rows from SQLite via bridge → POSTs to FastAPI server in batches
3. On success, watermark advanced; `onStatusUpdate` closure updates `@MainActor` state in `GooseAppModel`

### Live Activity / Strain Path

1. Workout begins → `beginActivityRecording()` resets `GooseStrainAccumulator`
2. Each `onLiveHeartRate` callback → `await strainAccumulator.addSample(bpm:)` (actor-isolated)
3. `liveWorkoutStrain: Double` on `GooseAppModel` updated; Live Activity content state pushed via `ActivityKit`
4. `GooseWorkoutLiveActivityWidget` renders in Dynamic Island / lock screen

**State Management:**
- Observable state: `GooseAppModel` and `HealthDataStore` `@Observable` properties on `@MainActor`
- Navigation state: `AppRouter` `@Published` properties
- Persistence: `UserDefaults` for onboarding/device identity; `goose.sqlite` for all health/packet/metric/journal/workout/apple_daily/metric_series data
- Database path resolved by `HealthDataStore.defaultDatabasePath()` → `ApplicationSupport/GooseSwift/goose.sqlite`

## Key Abstractions

**JSON-RPC FFI Bridge (`GooseRustBridge`):**
- Request schema: `{ "schema": "goose.bridge.request.v1", "method": "<name>", "args": {...} }`
- Response schema: `{ "ok": Bool, "result": Any, "error": {...}, "timing": {...} }`
- Sync call: `bridge.request(method:args:)` — blocks calling thread
- Async call: `bridge.requestAsync(method:args:)` — wraps in `Task.detached(priority: .userInitiated)`
- Testability: `GooseRustBridging` protocol at `GooseSwift/GooseRustBridging.swift`; `GooseRustBridge` retroactively conforms via `extension GooseRustBridge: GooseRustBridging {}`

**Extension-per-Concern Split (all three major classes):**
- `GooseAppModel`: `+NotificationPipeline`, `+Upload`, `+SleepSync`, `+BandFirstSync`, `+ActivityRecording`, `+ActivityTimeline`, `+HealthCapture`, `+Lifecycle`, `+PacketPublishing`
- `CoreBluetoothBLETransport`: `+CentralDelegate`, `+Commands`, `+DebugAndSync`, `+Haptics`, `+HistoricalCommands`, `+HistoricalHandlers`, `+HRMonitor`, `+Parsing`, `+PeripheralDelegate`, `+UserActions`, `+VitalsAndLogging`
- `HealthDataStore`: `+ActivitySnapshots`, `+BaselineProgress`, `+Cardio`, `+CoachSummaries`, `+Exercise`, `+IMUSteps`, `+PacketInputs`, `+Readiness`, `+Recovery`, `+Sleep`, `+Snapshots`, `+StagingSleep`, `+StaticSnapshots`, `+StressEnergy`, `+Trends`, `+Utilities`, `+V24Biometrics`, `+Vitals`

**Testability Protocols (added Phase 72):**
- `GooseRustBridging` — `GooseSwift/GooseRustBridging.swift`
- `GooseBLEManaging` — `GooseSwift/GooseBLEManaging.swift`
- `HealthDataStoring` — `GooseSwift/HealthDataStoring.swift`
- Unit tests: `WorkoutEntryTests`, `TrendsFetchTests`

**Shared ActivityKit Contract:**
- `WorkoutLiveActivityAttributes` at `GooseSwift/WorkoutLiveActivityAttributes.swift` — shared between main target and widget extension via `TARGET_MEMBERSHIP`

## Entry Points

**App Entry:**
- Location: `GooseSwift/GooseSwiftApp.swift`
- Triggers: iOS `@main`
- Responsibilities: Creates `GooseAppModel` and `AppRouter` as `@State`; injects into environment; handles `scenePhase` and `gooseswift://` deep links

**Widget Extension Entry:**
- Location: `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift`
- Triggers: WidgetKit extension process launch

**Rust FFI Entry:**
- Symbol: `goose_bridge_handle_json(const char*) -> char*`
- Header: `Rust/core/include/goose_core_bridge.h`
- Dispatch: `Rust/core/src/bridge.rs` matches `"method"` string against 58+ registered handler branches

## Architectural Constraints

- **Threading:** `@MainActor` for all UI and `@Observable` state mutations. Named background `DispatchQueue` instances: `com.goose.swift.notification-ingest`, `com.goose.swift.notification-parse`, `com.goose.swift.capture-frame-row-build`, `com.goose.swift.activity-timeline-refresh`, `com.goose.swift.capture-status-snapshot`, `com.goose.swift.rust-startup`. `NSLock` used for counters shared across queues.
- **Rust bridge is synchronous and blocking:** `goose_bridge_handle_json` blocks the calling thread. Never call from `@MainActor` directly for expensive operations. Always wrap in `Task.detached` or `requestAsync(method:)`.
- **Multiple bridge instances by design:** Do not attempt to share a single `GooseRustBridge`. Each consumer (coordinator, metric store, write queues) holds its own.
- **Database path convention:** Always pass `database_path: HealthDataStore.defaultDatabasePath()` explicitly in every bridge call that needs storage. Schema version: 20 (Phase 69).
- **Global state:** `HeartRateSeriesStore.shared` (`GooseSwift/HeartRateSeriesStores.swift`) is the only module-level singleton.
- **Circular imports:** None detected.
- **Extension target isolation:** `GooseWorkoutLiveActivityExtension` has no access to `GooseAppModel` or `GooseRustBridge`. Only `WorkoutLiveActivityAttributes.swift` is shared.
- **Raw Export OOM risk:** `Rust/core/src/export.rs` uses `fs::read()` which loads the entire SQLite DB into RAM before zipping — causes iOS jetsam kill. Mitigation: exclude sqlite from default export families and disable `includeRawBytes` by default.

## Anti-Patterns

### Calling GooseRustBridge.request() from @MainActor inline

**What happens:** Synchronous `bridge.request(method:)` called directly in a `@MainActor` method body without dispatching to a background thread.
**Why it's wrong:** Blocks the main thread for the full Rust execution duration; freezes UI; some `bridge.rs` methods take O(seconds) for full metric runs.
**Do this instead:** Use `bridge.requestAsync(method:)` (wraps in `Task.detached`) or `Task.detached(priority: .userInitiated) { try self.bridge.request(...) }` then `await MainActor.run { ... }`.

### Constructing ad-hoc GooseRustBridge() per call site

**What happens:** A one-off `GooseRustBridge()` is constructed inside a function body for a single call.
**Why it's wrong:** Bridge instances carry an `NSLock` and counter; per-call construction wastes resources and loses timing telemetry continuity.
**Do this instead:** Use the owning type's existing instance (`GooseAppModel.rust`, `HealthDataStore.bridge`). For new consumers, inject via constructor.

## Error Handling

**Strategy:** Errors surface as human-readable status strings on `@Observable` state properties, not propagated to the UI as thrown errors.

**Patterns:**
- Bridge failures: `catalogStatus = "Metric catalog unavailable: \(error)"` (representative pattern in `HealthDataStore`)
- BLE errors: logged via `ble.record(level: .error, source:, title:, body:)` and update `connectionState`
- Upload failures: stored in `uploadErrorState: String?` on `GooseAppModel`
- Bridge error type: `GooseRustBridgeError` enum — `encodingFailed`, `nullResponse`, `malformedResponse`, `methodFailed(String)`

## Cross-Cutting Concerns

**Logging:** `OSLog` with `ble.record(level:source:title:body:)` wrapper in BLE layer; status string pattern in coordinator/metric layers.
**Validation:** `MovementPacketValidation` type in coordinator layer; `GooseDataValidator` (Phase 68); Rust-side `validate_commands` bridge method.
**Authentication:** OAuth tokens in iOS Keychain via `Security` framework at `GooseSwift/CodexEmbeddedAuth.swift`; APNs device token on `GooseAppModel.apnsDeviceToken`.
**Upload gating:** `GooseNetworkMonitor` reachability + APNs token presence gate upload triggers in `GooseAppModel+Upload.swift`.

---

*Architecture analysis: 2026-06-13*
