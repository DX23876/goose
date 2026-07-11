---
focus: quality
last_mapped: 2026-06-13
---
# Coding Conventions

**Analysis Date:** 2026-06-13

## Naming Patterns

**Files:**
- PascalCase matching the primary type: `CoreBluetoothBLETransport.swift`, `ActivityModels.swift`, `HealthDataStore.swift`
- Extension files use `+` suffix: `CoreBluetoothBLETransport+Commands.swift`, `GooseAppModel+Upload.swift`, `HealthDataStore+Sleep.swift`
- View collections use `Views` suffix: `HealthDashboardViews.swift`, `SleepV2BevelTrendViews.swift`
- Type definition files use `Types` suffix: `GooseBLETypes.swift`, `CoachChatTypes.swift`
- Model files use `Models` suffix: `ActivityModels.swift`, `OnboardingModels.swift`

**Types:**
- PascalCase throughout: `GooseAppModel`, `CoreBluetoothBLETransport`, `OvernightSQLiteMirrorQueue`
- Error types append `Error` suffix: `GooseRustBridgeError`, `OpenAIResponsesError`
- Domain/subsystem prefix for disambiguation: `GooseMessage`, `GooseSyncToast`

**Functions and methods:**
- camelCase: `handleNotification`, `startOvernightGuard`, `refreshActivityTimeline`
- Action verbs: `begin`, `start`, `stop`, `handle`, `refresh`, `resume`, `persist`, `publish`
- Factory methods prefixed `make` or descriptive verb: `makeRequest`, `build`

**Properties and variables:**
- camelCase: `bluetoothState`, `connectionState`, `liveHeartRateBPM`
- Booleans prefixed `is`, `can`, `has`, `should`: `isScanning`, `canSend`, `isStreaming`, `batteryIsCharging`

**Constants:**
- `static let` on the enclosing type, camelCase: `static let bleUIStatePublishInterval: TimeInterval = 0.2`
- Enum cases used as namespaced constants: `OnboardingStorage.onboardingComplete`
- UserDefaults keys: dot-namespaced reverse-DNS strings, e.g. `"goose.swift.liveHRVRMSSD"`, `"goose.coach.modelPreset"`
- DispatchQueue labels: reverse-DNS format, e.g. `"com.goose.swift.corebluetooth"`, `"com.goose.swift.notification-ingest"`

**Enum cases:**
- camelCase: `case debug`, `case poweredOn`, `case healthMonitor`

## Code Style

**Formatting:**
- No formatter config file (no `.swiftformat`, `.swiftlint.yml`, or `biome.json`)
- 2-space indentation throughout all Swift files
- Opening braces on the same line as the declaration (K&R style)
- Trailing commas in multi-line array/dict literals
- One blank line between methods within a type
- Two blank lines between top-level declarations in extension files (after import block)
- No blank lines between `import` statements
- Long method signatures: each parameter on its own indented line, closing `)` on its own line

**Access control:**
- `private` used heavily for internal state in `final class` types
- `private(set)` for read-only public properties: `@Published private(set) var messages`
- `nonisolated` on static utility methods safe to run off the main actor
- `@unchecked Sendable` on queue-protected types: `final class CaptureFrameWriteQueue: @unchecked Sendable`

## Import Organization

- Each framework on its own `import` line
- No blank lines between imports
- Alphabetical ordering not strictly enforced

## Threading Rules

**Main thread (`@MainActor`):**
- All UI mutations and `@Published` state changes
- `GooseAppModel` and `HealthDataStore` are both `@MainActor final class`
- Background work dispatches back via `Task { @MainActor in ... }`

**Background queues (dedicated `DispatchQueue` instances):**
- BLE events: `"com.goose.swift.corebluetooth"`
- Notification parsing: `"com.goose.swift.notification-ingest"`
- Frame write: dedicated queue in `CaptureFrameWriteQueue`
- Overnight mirror: `OvernightSQLiteMirrorQueue`

**Rust bridge calls:**
- `goose_bridge_handle_json` is synchronous and blocks the calling thread
- Never call from `@MainActor` with expensive methods
- Always dispatch to a background queue first, then surface results back to main
- Async wrappers (`requestAsync`, `requestValueAsync`) use `Task.detached(priority: .userInitiated)`

**Shared-counter protection:**
- `NSLock` for counters shared between queues

## Error Handling

**Bridge failures:**
- Set human-readable status strings published as `@Published` properties
- Pattern: `catalogStatus = "Metric catalog unavailable: \(error)"`
- Bridge `request(method:args:)` throws; callers use `try?` or `do/catch`

**BLE errors:**
- Logged via `logger.record(level: .error, ...)` and update `connectionState` string

**Overnight guard:**
- Errors accumulate as warning strings in `overnightGuardWarning` and `overnightGuardStatus`

**General pattern:**
- `guard ... else { return }` for early exits
- `try?` discards non-critical errors; `try` with `catch` when the error must be surfaced

## Logging

**Framework:** `OSLog` via `Logger`

**Declaration pattern (file-private logger per module):**
```swift
import OSLog

private let logger = Logger(subsystem: "com.goose.swift", category: "upload")
// or as an instance property:
let logger = Logger(subsystem: "com.goose.swift", category: "ble")
```

**Category strings by subsystem:**
- BLE: `"ble"` — `GooseSwift/CoreBluetoothBLETransport.swift`, `GooseSwift/GooseBLEDataValidator.swift`
- Upload: `"upload"` — `GooseSwift/GooseUploadService.swift`

**Level usage:**
- `.debug` — operational flow, bridge call results, retry counts
- `.warning` — failures that need visibility in production (e.g. `sync.mark_synced` failures)
- `.error` — BLE delegate errors, unrecoverable states

## Comments

- Inline `//` comments explain non-obvious logic or configuration constants
- `///` doc comments are NOT used — no public API documentation in this codebase
- No TODO, FIXME, HACK, or XXX markers in the codebase
- Comments use natural sentence case

## Module / Type Design

**Large types split by concern using extension files:**
- `CoreBluetoothBLETransport` → `+Commands`, `+HistoricalCommands`, `+Parsing`, `+PeripheralDelegate`, `+VitalsAndLogging`
- `GooseAppModel` → `+NotificationPipeline`, `+ActivityRecording`, `+OvernightRun`, `+Upload`
- `HealthDataStore` → `+PacketInputs`, `+Snapshots`, `+Sleep`, `+Cardio`

**Singleton:**
- `HeartRateSeriesStore.shared` (`GooseSwift/HeartRateSeriesStores.swift`) is the only module-level singleton
- All other state is instance-owned

**Database path convention:**
- Always `ApplicationSupport/GooseSwift/goose.sqlite`, resolved via `HealthDataStore.defaultDatabasePath()`
- Pass explicitly as `database_path` argument in every bridge call that needs storage

---

*Convention analysis: 2026-06-13*
