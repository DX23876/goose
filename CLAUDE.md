<!-- GSD:project-start source:PROJECT.md -->
## Project

**Goose — Self-Hosted Biometric Platform**

iOS app (SwiftUI + Rust core) that reads biometric data from WHOOP devices via BLE and persists it to a self-hosted server.

**Core Value:** Users must be able to capture WHOOP data on iPhone and have it automatically persisted on their personal server — without depending on external infrastructure.

### Constraints

- **iOS stack**: Swift / SwiftUI / URLSession — no external dependencies
- **Server stack**: FastAPI + TimescaleDB (Docker, self-hosted)
- **Git**: planning docs committed (commit_docs: true)
<!-- GSD:project-end -->

## Common Commands

### iOS build (simulator)
```bash
xcodebuild -project GooseSwift.xcodeproj -scheme GooseSwift -configuration Debug \
  -destination 'platform=iOS Simulator,name=iPhone 17' \
  -derivedDataPath /tmp/goose-swift-deriveddata build
```

### iOS build (physical device)
```bash
xcodebuild -project GooseSwift.xcodeproj -scheme GooseSwift -configuration Debug \
  -destination 'platform=iOS,id=<device-id>' \
  -derivedDataPath /tmp/goose-swift-deriveddata-device -allowProvisioningUpdates build
```
List device IDs: `xcrun devicectl list devices`

### Rust core only (cross-compile for iOS)
```bash
PLATFORM_NAME=iphonesimulator CURRENT_ARCH=arm64 Scripts/build_ios_rust.sh   # simulator
PLATFORM_NAME=iphoneos CURRENT_ARCH=arm64 Scripts/build_ios_rust.sh          # device
```
The `.a` archives are gitignored; Xcode runs this script automatically as a build phase. Set `GOOSE_SKIP_RUST_CORE_BUILD=1` to skip recompilation when the archive is already current.

### Tests
```bash
# Rust — all tests
cargo test --manifest-path Rust/core/Cargo.toml
# Rust — single test file
cargo test --manifest-path Rust/core/Cargo.toml --test protocol_tests
# Rust — with stdout
cargo test --manifest-path Rust/core/Cargo.toml -- --nocapture

# Swift — GooseSwiftTests target, via xcodebuild
xcodebuild test -project GooseSwift.xcodeproj -scheme GooseSwift \
  -destination 'platform=iOS Simulator,name=iPhone 17' \
  -derivedDataPath /tmp/goose-swift-deriveddata
```
Swift tests can also be run from Xcode (Product → Test). There is no single-test CLI invocation for Swift; use `-only-testing:GooseSwiftTests/<ClassName>/<methodName>` with `xcodebuild test` to scope to one test.

### Lint / format (Rust)
```bash
cd Rust/core && cargo fmt          # format
cd Rust/core && cargo fmt -- --check   # CI check, no changes
cd Rust/core && cargo clippy       # lint (non-blocking in CI, but don't add new warnings)
cd Rust/core && cargo check        # fast type-check
```
Swift has no formatter/linter config in this repo (no `.swiftformat`/`.swiftlint.yml`) — match surrounding file style by hand.

### Self-hosted server
```bash
cd server && cp .env.example .env   # set GOOSE_API_KEY, GOOSE_DB_PASSWORD
docker compose up -d --build
curl -s localhost:8770/healthz      # → {"status":"ok"}
```

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Swift 5.0 — iOS app, all UI and business logic in `GooseSwift/`, live activity extension in `GooseWorkoutLiveActivityExtension/`
- Rust (Edition 2024, MSRV 1.96) — Rust core library in `Rust/core/src/`, protocol parsing, metric computation, SQLite persistence, FFI bridge
- Python — Reference algorithm scripts only (`Rust/core/tools/reference/*.py`); not used at runtime
- Bash — Rust cross-compilation script at `Scripts/build_ios_rust.sh`
## Runtime
- iOS 26.0 (deployment target — `IPHONEOS_DEPLOYMENT_TARGET = 26.0` in `GooseSwift.xcodeproj/project.pbxproj`)
- ARM64 device (`aarch64-apple-ios`), ARM64 simulator (`aarch64-apple-ios-sim`), x86_64 simulator (`x86_64-apple-ios`) all supported via build script
- Swift: No SPM (no root `Package.swift`). Project managed via `GooseSwift.xcodeproj`. Two local packages exist in `Packages/WhoopProtocol/` and `Packages/WhoopStore/` but contain only `.swiftpm` metadata — no source files; they appear to be placeholder or removed packages.
- Rust: Cargo, lockfile at `Rust/core/Cargo.lock` (present, committed)
## Frameworks
- SwiftUI — all UI; 80 files import SwiftUI
- UIKit — used for appearance configuration and low-level UI hooks; 81 files import UIKit
- Foundation — universal; 97 files import Foundation
- CoreBluetooth — BLE communication with WHOOP device; 14 files import CoreBluetooth
- HealthKit — body mass autofill from Apple Health; 11 files import HealthKit; entitlement `com.apple.developer.healthkit` granted in `GooseSwift/GooseSwift.entitlements`
- CoreLocation + MapKit — outdoor workout GPS tracking; 12 files import CoreLocation, 9 import MapKit
- ActivityKit — Live Activity / Dynamic Island for workouts; `GooseSwift/WorkoutLiveActivityController.swift`, `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift`
- WidgetKit — Live Activity widget extension; `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift`
- OSLog — structured logging; 11 files import OSLog
- CryptoKit — SHA-256 file integrity checksums for export; 5 files import CryptoKit
- Security — iOS Keychain for OAuth token storage; `GooseSwift/CodexEmbeddedAuth.swift`
- UserNotifications — notification permission onboarding; `GooseSwift/OnboardingModels.swift`, `GooseSwift/OnboardingPermissions.swift`
- Rust: Cargo's built-in test runner (`cargo test`). Integration tests in `Rust/core/tests/` (47 files). Swift tests in `GooseSwiftTests/` target (69 tests across 16 files).
- Xcode project: `GooseSwift.xcodeproj`
- Rust cross-compile: `Scripts/build_ios_rust.sh` — invoked as an Xcode build phase, produces `Rust/iphoneos/libgoose_core.a` and `Rust/iphonesimulator/libgoose_core.a`
- Python reference tools: `Rust/core/tools/reference/` — neurokit2, pyhrv, pyactigraphy, ggir; used only for algorithm validation/comparison, not production
## Key Dependencies
- `rusqlite 0.37` (feature: `bundled`) — SQLite embedded in the static library; all health/capture/activity persistence goes through this
- `serde 1.0` + `serde_json 1.0` — all JSON serialisation for the FFI bridge protocol
- `tungstenite 0.28` — WebSocket server used for local debug sessions (`ws://127.0.0.1:8765`)
- `zip 0.6` — raw data export bundling
- `sha2 0.10` — SHA-256 digests inside Rust (separate from Swift CryptoKit usage)
- `crc32fast 1.4` — CRC32 frame checksums
- `hex 0.4` — hex encoding for BLE frame capture
- `thiserror 2.0` — error type derivation
- `tempfile 3.13` (dev-only) — test temporary files
## Configuration
- No `.env` files. Configuration driven by `ProcessInfo.processInfo` launch arguments and environment variables at runtime:
- `GooseSwift.xcodeproj` — main Xcode project
- `Scripts/build_ios_rust.sh` — Rust cross-compilation invoked as Xcode build phase; reads `PLATFORM_NAME`, `CONFIGURATION`, `CURRENT_ARCH`, `IPHONEOS_DEPLOYMENT_TARGET` from Xcode environment
- Bundle ID: `com.goose.app` (main app), `com.goose.app.WorkoutLiveActivityExtension` (extension)
- Marketing version: `8.0`, build: `8`
- URL scheme: `gooseapp://` (`CFBundleURLSchemes` in `GooseSwift/Info.plist`)
## Platform Requirements
- macOS with Xcode (iOS 26.0 SDK)
- Rust toolchain with targets: `aarch64-apple-ios`, `aarch64-apple-ios-sim`, `x86_64-apple-ios`
- Cargo (installed separately or via rustup)
- iOS device or simulator, iOS 26.0+
- Static libraries `Rust/iphoneos/libgoose_core.a` and `Rust/iphonesimulator/libgoose_core.a` are **gitignored** — built automatically by Xcode via `Scripts/build_ios_rust.sh` build phase
- Bluetooth background mode required (`UIBackgroundModes: bluetooth-central`)
- Location background mode required (`UIBackgroundModes: location`)
- Local networking allowed (`NSAllowsLocalNetworking: true`) for debug WebSocket
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Swift source files use PascalCase matching the primary type they contain: `CoreBluetoothBLETransport.swift`, `ActivityModels.swift`, `HealthDataStore.swift`
- Extensions that add a functional area to a class use `+` suffix notation: `CoreBluetoothBLETransport+Commands.swift`, `GooseAppModel+OvernightRun.swift`, `HealthDataStore+Utilities.swift`
- Views use a `Views` suffix for files containing multiple related views: `HealthDashboardViews.swift`, `SleepV2BevelTrendViews.swift`
- Type definition files use a `Types` suffix: `GooseBLETypes.swift`, `CoachChatTypes.swift`, `HealthPacketCaptureTypes.swift`
- Models use a `Models` suffix: `ActivityModels.swift`, `HealthModels.swift`, `OnboardingModels.swift`
- PascalCase throughout: `GooseAppModel`, `CoreBluetoothBLETransport`, `OvernightSQLiteMirrorQueue`
- Prefix with the subsystem or domain name for disambiguation: `GooseMessage`, `GooseSyncToast`, `GooseHistoricalSyncProgress`
- Enum cases use camelCase: `case debug`, `case poweredOn`, `case healthMonitor`
- Error types use PascalCase with an `Error` suffix: `GooseRustBridgeError`, `OpenAIResponsesError`
- camelCase: `handleNotification`, `startOvernightGuard`, `refreshActivityTimeline`
- Verbs for actions: `begin`, `start`, `stop`, `handle`, `refresh`, `resume`, `persist`, `publish`
- Booleans prefixed with `is`, `can`, `has`, `should`: `isScanning`, `canSend`, `isStreaming`
- Factory static methods prefixed with `make` or descriptive verbs: `makeRequest`, `build`
- camelCase for all stored and computed properties: `bluetoothState`, `connectionState`, `liveHeartRateBPM`
- UserDefaults keys use dot-namespaced reverse-DNS strings stored as `static let` on the relevant type: `"goose.swift.liveHRVRMSSD"`, `"goose.coach.modelPreset"`
- DispatchQueue labels use reverse-DNS format: `"com.goose.swift.corebluetooth"`, `"com.goose.swift.notification-ingest"`
- `static let` on the enclosing type; naming is camelCase: `static let bleUIStatePublishInterval: TimeInterval = 0.2`, `static let maximumDisplayedMessages = 300`
- Enum cases used as namespaced constants: `OnboardingStorage.onboardingComplete`, `FitnessColor.workoutYellow`
## Code Style
- No formatter config file detected (no `.swiftformat`, `.swiftlint.yml`, or similar)
- 2-space indentation used consistently throughout all Swift files
- Opening braces on the same line as the declaration (Allman-adjacent, K&R style)
- Trailing commas in multi-line array/dict literals
- One blank line between methods within a type
- Two blank lines between top-level declarations in an extension file (import block + two blank lines + extension body)
- No blank lines between `import` statements
- Long method signatures split with each parameter on its own indented line, closing `)` on its own line:
- `private` used heavily for internal state in `final class` types (~1281 occurrences)
- `private(set)` used for read-only public properties in `ObservableObject`: `@Published private(set) var messages`
- `nonisolated` used on static utility methods that can safely run off the main actor: `nonisolated static func writeRawValidationSidecars(...)`
- `@unchecked Sendable` on queue-protected types: `final class CaptureFrameWriteQueue: @unchecked Sendable`
## Import Organization
- Each framework on its own `import` line
- No blank lines between imports
- Alphabetical ordering within each import group is not strictly enforced
## Error Handling
## Logging
## Comments
- Inline documentation comments (`///`) are not used on public API — this codebase has no `///` doc comments
- Inline `//` comments explain non-obvious logic or configuration constants
- No TODO, FIXME, HACK, or XXX markers found in the codebase
- Explanatory comments use natural sentence case
- Parameter names and type context are omitted in comments; they are considered self-documenting from the code
## Function Design
## Module / Type Design
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

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
│  CoreBluetoothBLETransport │     │  GooseRustBridge               │
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
| Component | Responsibility | File |
|-----------|----------------|------|
| `GooseSwiftApp` | App entry point; scene config; lifecycle events | `GooseSwift/GooseSwiftApp.swift` |
| `GooseAppModel` | Central coordinator; owns BLE client, Rust bridge, packet pipelines, overnight guard | `GooseSwift/GooseAppModel.swift` + `GooseAppModel+*.swift` |
| `CoreBluetoothBLETransport` | CoreBluetooth central; WHOOP GATT connection; packet framing; command writes | `GooseSwift/CoreBluetoothBLETransport.swift` + `CoreBluetoothBLETransport+*.swift` |
| `GooseRustBridge` | JSON-over-FFI bridge to Rust; serialises requests, deserialises responses, tracks timing | `GooseSwift/GooseRustBridge.swift` |
| `HealthDataStore` | Rust bridge consumer for metric scores; @MainActor; owns packet input reports | `GooseSwift/HealthDataStore.swift` + `HealthDataStore+*.swift` |
| `AppRouter` | Tab selection, deep-link handling, navigation paths | `GooseSwift/AppRouter.swift` |
| `RootView` | Onboarding gate; renders either `OnboardingView` or `AppShellView` | `GooseSwift/RootView.swift` |
| `AppShellView` | Tab bar with Home/Health/Coach/More | `GooseSwift/AppShellView.swift` |
| `NotificationFrameParser` | Delegates raw BLE bytes to Rust for frame parsing; compact summary extraction | `GooseSwift/NotificationFrameParsing.swift` |
| `CaptureFrameWriteQueue` | Batched SQLite inserts of captured BLE frames via Rust bridge | `GooseSwift/CaptureFrameWriteQueue.swift` |
| `OvernightSQLiteMirrorQueue` | During overnight guard, queues raw notification rows → Rust bridge insert | `GooseSwift/OvernightSQLiteMirrorQueue.swift` |
| `WhoopDataSignalPipeline` | Ingests `WhoopDataSignalSample` on a dedicated queue; forwards to aggregators | `GooseSwift/WhoopDataSignalPipeline.swift` |
| `PassiveActivityDetectionPipeline` | Heuristic motion/HR analysis to auto-detect workout sessions | `GooseSwift/PassiveActivityDetector.swift` |
| `WorkoutLiveActivityController` | Manages `ActivityKit` Live Activity lifecycle for workouts | `GooseSwift/WorkoutLiveActivityController.swift` |
| Rust core (bridge) | Protocol parsing, SQLite persistence, metric algorithms, BLE frame import | `Rust/core/src/bridge/mod.rs` (154 dispatched methods across domain files) |
| `GooseWorkoutLiveActivityWidget` | WidgetKit / ActivityKit extension; renders Dynamic Island + lock-screen UI | `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift` |
## Pattern Overview
- `GooseAppModel` is the single `@MainActor` coordinator; UI observes it via `@EnvironmentObject`.
- The Rust library (`libgoose_core`) is stateless from Swift's perspective; state is persisted in SQLite. Each bridge call passes the `database_path` argument.
- BLE bytes flow inward through callbacks on `CoreBluetoothBLETransport`, are reassembled into frames on `notificationIngestQueue`, parsed via `GooseRustBridge`, then written to SQLite via `CaptureFrameWriteQueue`.
- Thread safety: `@MainActor` for all UI mutations; dedicated `DispatchQueue` instances for BLE, parse, write, and pipeline work; `NSLock` guards for shared counters.
## Layers
- Purpose: SwiftUI rendering, user interaction
- Location: `GooseSwift/` (all `*View.swift`, `*Views.swift`, `*Screen.swift`)
- Contains: SwiftUI `View` structs, view-local `@State`
- Depends on: `GooseAppModel`, `HealthDataStore`, `AppRouter` via `@EnvironmentObject`/`@StateObject`
- Used by: `AppShellView` tab builder
- Purpose: Business logic, state machine, pipeline wiring
- Location: `GooseSwift/GooseAppModel.swift` + `GooseAppModel+*.swift`
- Contains: `@MainActor final class GooseAppModel: ObservableObject`; all `@Published` state; extension files split by concern
- Depends on: `CoreBluetoothBLETransport`, `GooseRustBridge`, dispatch queues, `NotificationFrameParser`, `CaptureFrameWriteQueue`
- Used by: `GooseSwiftApp`, SwiftUI views
- Purpose: Query Rust bridge for scored metrics; publish results to views
- Location: `GooseSwift/HealthDataStore.swift` + `HealthDataStore+*.swift`
- Contains: `@MainActor final class HealthDataStore: ObservableObject`; owns a `GooseRustBridge` instance
- Depends on: `GooseRustBridge` (each method call passes `database_path`)
- Used by: `AppShellView` (creates one instance), view tabs
- Purpose: CoreBluetooth central manager; WHOOP GATT protocol; command writes and notifications
- Location: `GooseSwift/CoreBluetoothBLETransport.swift` + `CoreBluetoothBLETransport+*.swift`
- Contains: `CBCentralManagerDelegate`, `CBPeripheralDelegate`; proprietary WHOOP command framing
- Depends on: CoreBluetooth, OSLog
- Used by: `GooseAppModel` (holds the instance)
- Purpose: Type-safe JSON envelope around a C FFI function pair
- Location: `GooseSwift/GooseRustBridge.swift`, `GooseSwift/GooseSwift-Bridging-Header.h`
- Contains: `GooseRustBridge` class; calls `goose_bridge_handle_json` / `goose_bridge_free_string`
- Depends on: `Rust/core/include/goose_core_bridge.h` (two C symbols)
- Used by: `GooseAppModel` (one instance), `HealthDataStore` (own instance), `OvernightSQLiteMirrorQueue` (own instance), `CaptureFrameWriteQueue` (own instance), ad-hoc calls in extensions
- Purpose: Protocol parsing, SQLite schema and persistence, metric feature extraction, health scoring algorithms
- Location: `Rust/core/src/`
- Contains: 40+ Rust modules; entry point `bridge.rs` dispatches JSON `method` strings to internal functions
- Depends on: `rusqlite`, `serde_json`, `serde`; writes to `goose.sqlite`
- Used by: Swift side only through the C FFI pair
- Purpose: Live Activity (Dynamic Island + lock screen) for active workouts
- Location: `GooseWorkoutLiveActivityExtension/`
- Contains: `GooseWorkoutLiveActivityWidget`, `WorkoutLiveActivityAttributes` (shared type)
- Depends on: `ActivityKit`, `WidgetKit`; reads `WorkoutLiveActivityAttributes.ContentState` pushed from main app
## Data Flow
### Primary Real-Time BLE → SQLite Path
### Metric Score Path (on-demand)
### Overnight Guard Path
### Live Activity Path
- All observable state lives in `GooseAppModel` and `HealthDataStore` as `@Published` properties on `@MainActor`
- Navigation state lives in `AppRouter`
- Persistence: `UserDefaults` for onboarding/device identity/HR estimates; `goose.sqlite` for all health/packet data; `ApplicationSupport/GooseSwift/` for database and logs; `Documents/GooseSwift/` for user-accessible exports
## Key Abstractions
- Purpose: JSON-RPC envelope over a single C function `goose_bridge_handle_json`. Schema: `goose.bridge.request.v1` with `method` + `args`. Rust returns `{ok, result, error, timing}`.
- Examples: `GooseSwift/GooseRustBridge.swift` (lines 26–81)
- Pattern: Each caller creates its own bridge instance; bridge is stateless. Always pass `database_path` in args for storage-backed methods.
- Purpose: Large class split into focused extension files by concern
- Examples: `CoreBluetoothBLETransport+Commands.swift`, `CoreBluetoothBLETransport+HistoricalCommands.swift`, `CoreBluetoothBLETransport+Parsing.swift`, `CoreBluetoothBLETransport+PeripheralDelegate.swift`
- Pattern: Each extension file owns a coherent slice of BLE behaviour; all share state on the parent class
- Purpose: Coordinator split across extension files by domain
- Examples: `GooseAppModel+NotificationPipeline.swift`, `GooseAppModel+ActivityRecording.swift`, `GooseAppModel+OvernightRun.swift`
- Pattern: Concern-scoped extensions on `@MainActor` class; background queue work dispatches back to main via `Task { @MainActor in ... }`
- Purpose: Query layer split by metric family
- Examples: `HealthDataStore+PacketInputs.swift`, `HealthDataStore+Snapshots.swift`, `HealthDataStore+Sleep.swift`, `HealthDataStore+Cardio.swift`
- Pattern: Each extension calls `bridge.request(method: "metrics.*")` with `database_path` arg; updates `@Published` state
- Purpose: Shared type between main app and WidgetKit extension (ActivityKit contract)
- Examples: `GooseSwift/WorkoutLiveActivityAttributes.swift`
- Pattern: `ActivityAttributes` conformance; `ContentState` carries mutable workout metrics
## Entry Points
- Location: `GooseSwift/GooseSwiftApp.swift`
- Triggers: iOS app launch (`@main`)
- Responsibilities: Creates `GooseAppModel` and `AppRouter` as `@StateObject`; injects into environment; handles `scenePhase` changes and deep links
- Location: `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift`
- Triggers: WidgetKit extension process launch
- Responsibilities: Declares `GooseWorkoutLiveActivityWidget` for ActivityKit
## Architectural Constraints
- **Threading:** Main thread (`@MainActor`) for all UI and `@Published` state mutations. Background `DispatchQueue` instances for BLE events, notification parsing, frame row building, packet input computation, and overnight mirror writes. `NSLock` used for counters shared between queues.
- **Global state:** `HeartRateSeriesStore.shared` is a module-level singleton (`GooseSwift/HeartRateSeriesStores.swift`). All other state is instance-owned.
- **Rust bridge is synchronous:** `goose_bridge_handle_json` blocks the calling thread. Never call from `@MainActor` with expensive methods; always dispatch to a background queue first.
- **Database path convention:** The SQLite file is always at `ApplicationSupport/GooseSwift/goose.sqlite`, resolved via `HealthDataStore.defaultDatabasePath()`. Pass this path explicitly in every bridge call that needs storage.
- **Multiple bridge instances:** `GooseRustBridge` is not a singleton; `GooseAppModel`, `HealthDataStore`, `OvernightSQLiteMirrorQueue`, and `CaptureFrameWriteQueue` each hold their own instance. This is intentional — the Rust side is stateless across calls.
- **Circular imports:** None detected.
- **Extension target isolation:** `GooseWorkoutLiveActivityExtension` shares `WorkoutLiveActivityAttributes.swift` with the main target. It has no access to `GooseAppModel` or `GooseRustBridge`.
## Anti-Patterns
### Calling GooseRustBridge from @MainActor inline
### Constructing ad-hoc GooseRustBridge() per call site
## Error Handling
- Bridge failures set human-readable status strings (e.g., `catalogStatus = "Metric catalog unavailable: \(error)"`)
- BLE errors are logged via `ble.record(level: .error, ...)` and update `connectionState`
- Overnight guard errors accumulate as warning strings in `overnightGuardWarning` and `overnightGuardStatus`
## Cross-Cutting Concerns
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
