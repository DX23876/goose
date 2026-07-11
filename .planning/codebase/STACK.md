---
focus: tech
last_mapped: 2026-06-13
---
# Technology Stack

**Analysis Date:** 2026-06-13

## Languages

**Primary:**
- Swift 5.0 — all iOS app code in `GooseSwift/` and `GooseWorkoutLiveActivityExtension/`
- Rust (Edition 2024, MSRV 1.96) — protocol parsing, metric algorithms, SQLite persistence, FFI bridge in `Rust/core/src/`

**Secondary:**
- Python 3.12 — server ingest service in `server/ingest/`; reference algorithm scripts in `Rust/core/tools/reference/` (not runtime)
- Bash — Rust cross-compilation script at `Scripts/build_ios_rust.sh`

## Runtime

**iOS App:**
- iOS 26.0 deployment target (`IPHONEOS_DEPLOYMENT_TARGET = 26.0` in `GooseSwift.xcodeproj/project.pbxproj`)
- ARM64 device (`aarch64-apple-ios`), ARM64 simulator (`aarch64-apple-ios-sim`), x86_64 simulator (`x86_64-apple-ios`)

**Server:**
- Python 3.12 (FastAPI + uvicorn)
- Docker — TimescaleDB 2.17.2-pg16 container + ingest container
- `server/docker-compose.yml` defines both services

**Package Manager:**
- Swift: no SPM; project managed via `GooseSwift.xcodeproj`. Local packages at `Packages/WhoopProtocol/` and `Packages/WhoopStore/` exist but contain no source files.
- Rust: Cargo; lockfile at `Rust/core/Cargo.lock` (committed)
- Python: pip; `server/ingest/requirements.txt` and `server/ingest/requirements-dev.txt`

## iOS Frameworks

**Core UI:**
- SwiftUI — all UI; imported by 80+ files
- UIKit — appearance config and low-level hooks; 81 files

**System:**
- Foundation — universal; 97 files
- CoreBluetooth — BLE communication with WHOOP; `GooseSwift/CoreBluetoothBLETransport.swift` + `CoreBluetoothBLETransport+*.swift`; 14 files
- HealthKit — body mass autofill from Apple Health; `GooseSwift/GooseAppModel+HealthKit.swift`; 11 files
- CoreLocation + MapKit — GPS for outdoor workouts; 12 + 9 files
- Network — `NWPathMonitor` for reachability; `GooseSwift/GooseNetworkMonitor.swift`
- BackgroundTasks — `BGTaskScheduler` for deferred sync; `GooseSwift/GooseSwiftApp.swift`, `GooseSwift/GooseAppModel+BandFirstSync.swift`

**Live Activity / Notifications:**
- ActivityKit — Live Activity lifecycle; `GooseSwift/WorkoutLiveActivityController.swift`
- WidgetKit — Dynamic Island extension; `GooseWorkoutLiveActivityExtension/GooseWorkoutLiveActivityWidget.swift`
- UserNotifications — permission onboarding; `GooseSwift/OnboardingModels.swift`, `GooseSwift/OnboardingPermissions.swift`

**Security / Crypto:**
- Security — iOS Keychain for API key storage; `GooseSwift/CodexEmbeddedAuth.swift`, `GooseSwift/ClaudeCoachProvider.swift`
- CryptoKit — SHA-256 checksums for export; 5 files

**Observability:**
- OSLog — structured logging; 11 files; subsystem `com.goose.swift`

## Rust Library (FFI)

- Crate name: `goose-core` version `8.0.0`
- Crate types: `rlib`, `staticlib`, `cdylib`
- Source root: `Rust/core/src/lib.rs`
- Bridge dispatcher: `Rust/core/src/bridge.rs` (58+ dispatched methods)
- Static libraries (pre-built, committed): `Rust/iphoneos/libgoose_core.a`, `Rust/iphonesimulator/libgoose_core.a`
- FFI header: `GooseSwift/GooseSwift-Bridging-Header.h` (imports `Rust/core/include/goose_core_bridge.h`)
- Swift bridge wrapper: `GooseSwift/GooseRustBridge.swift`

## Key Rust Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `rusqlite` | 0.40 (bundled) | SQLite embedded storage (`goose.sqlite`) |
| `serde` + `serde_json` | 1.0 | JSON serialisation for FFI bridge protocol |
| `tungstenite` | 0.29 | WebSocket server for local debug sessions (`ws://127.0.0.1:8765`); non-Android only |
| `zip` | 8.6 | Raw data export bundling |
| `sha2` | 0.11 | SHA-256 digests |
| `crc32fast` | 1.4 | CRC32 BLE frame checksums |
| `hex` | 0.4 | Hex encoding for captured frames |
| `thiserror` | 2.0 | Error type derivation |
| `tempfile` | 3.13 (dev) | Test temporary files |

## Key Server (Python) Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `fastapi` | 0.136.3 | HTTP API framework |
| `uvicorn[standard]` | 0.49.0 | ASGI server |
| `psycopg[binary]` | 3.3.4 | PostgreSQL/TimescaleDB client |
| `pydantic` | (via fastapi) | Request validation |
| `neurokit2` | 0.2.13 | Sleep staging, HRV computation |
| `numpy` | 2.4.6 | Numerical computation |
| `scipy` | 1.17.1 | Signal processing |
| `scikit-learn` | 1.9.0 | ML for daily metrics |
| `pandas` | 2.3.3 | Data manipulation |
| `httpx` | >=0.28.1 | HTTP client (WHOOP official API) |
| `zstandard` | 0.25.0 | Compression |

## Build System

**iOS:**
- Xcode project: `GooseSwift.xcodeproj`
- Rust cross-compile: `Scripts/build_ios_rust.sh` — invoked as Xcode build phase; reads `PLATFORM_NAME`, `CONFIGURATION`, `CURRENT_ARCH`, `IPHONEOS_DEPLOYMENT_TARGET`; skips rebuild if inputs unchanged
- Bundle ID: `com.goose.swift` (app), `com.goose.swift.WorkoutLiveActivityExtension` (extension)
- Marketing version: `0.1.0`, build `1`
- URL scheme: `gooseswift://`

**Server:**
- Dockerfile: `server/ingest/Dockerfile`
- Docker Compose: `server/docker-compose.yml`
- Ingest port: `${GOOSE_INGEST_PORT:-8770}` → container port `8000`

## CI/CD

**Workflows:**

| Workflow | File | Trigger | Runner |
|----------|------|---------|--------|
| Swift Build | `.github/workflows/swift-build.yml` | push main (`GooseSwift/**`), all PRs | `macos-15` + Xcode 26.3 |
| Rust core | `.github/workflows/rust-core.yml` | push main (`Rust/**`), all PRs | `ubuntu-latest` + `macos-15` |
| Server CI | `.github/workflows/server-ci.yml` | push main (`server/**`), all PRs | `ubuntu-latest` |
| Security | `.github/workflows/security.yml` | scheduled | `ubuntu-latest` |
| CodeQL | `.github/workflows/codeql.yml` | scheduled + PRs | `ubuntu-latest` |
| Zizmor | `.github/workflows/zizmor.yml` | PRs | `ubuntu-latest` |
| Release | `.github/workflows/release.yml` | tag push | `macos-15` |

**Swift CI:** checkout → Xcode 26.3 select → `rustup target add aarch64-apple-ios-sim` → Cargo cache → `xcodebuild -scheme GooseSwift -sdk iphonesimulator CODE_SIGNING_ALLOWED=NO`

**Rust CI jobs:**
1. `cargo fmt --all -- --check` (blocks merge)
2. `cargo build --lib` + `cargo test --lib` on ubuntu-latest and macos-15 (blocks merge)
3. `cargo clippy --lib` — advisory only, `continue-on-error: true` (~120 warnings exist)

**Server CI:** Python 3.12 → pip install → Docker verify → `pytest tests/ -v --tb=short`

## Platform Requirements

**Development:**
- macOS with Xcode 26.3+ (iOS 26.0 SDK)
- Rust toolchain with targets: `aarch64-apple-ios`, `aarch64-apple-ios-sim`, `x86_64-apple-ios`
- iOS 26.0 device or simulator
- Docker (for server development)

**iOS Entitlements:**
- `com.apple.developer.healthkit`
- `UIBackgroundModes: bluetooth-central, location`
- `NSAllowsLocalNetworking: true` (debug WebSocket to Rust)

---

*Stack analysis: 2026-06-13*
