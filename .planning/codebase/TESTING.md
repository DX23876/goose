---
focus: quality
last_mapped: 2026-06-13
---
# Testing Patterns

**Analysis Date:** 2026-06-13

## Test Framework

**Swift (XCTest):**
- Runner: XCTest (via Xcode test target `GooseSwiftTests`)
- Config: `GooseSwiftTests/Info.plist`
- No vitest, jest, or separate config file
- Run: Build and test via Xcode or `xcodebuild test`

**Rust (cargo test):**
- Runner: Cargo built-in test runner
- Config: `Rust/core/Cargo.toml` (dev-dependency: `tempfile 3.13`)
- Run commands:
```bash
cargo test --manifest-path Rust/core/Cargo.toml           # All tests
cargo test --manifest-path Rust/core/Cargo.toml --test protocol_tests  # Single file
cargo test --manifest-path Rust/core/Cargo.toml -- --nocapture          # With stdout
```

## Test File Organization

**Swift tests:**
- Location: `GooseSwiftTests/` (flat directory, all at root)
- Naming: `{Subject}Tests.swift` (e.g. `GooseBLETypesTests.swift`, `GooseUploadServiceTests.swift`)
- Mocks/helpers: `MockRustBridge.swift`, `MockBLEClient.swift`, `MockHealthStore.swift`
- Import pattern: `@testable import GooseSwift` on every test file

**Rust integration tests:**
- Location: `Rust/core/tests/` (flat directory, 44 files)
- Naming: `{subject}_tests.rs` (snake_case, e.g. `bridge_tests.rs`, `protocol_tests.rs`)
- No separate fixtures directory; fixture data embedded inline or referenced via `build_fixture_index()`

## Test Counts

| Layer | Test functions |
|-------|---------------|
| Swift (XCTest) | ~69 |
| Rust integration (`Rust/core/tests/`) | ~792 `#[test]` functions |
| Rust unit (inline `#[cfg(test)]` in `src/`) | ~161 `#[test]` occurrences |
| **Total Rust** | **~953** |

## Swift Test Structure

**Suite organization:**
```swift
import XCTest
import CoreBluetooth
@testable import GooseSwift

final class GooseBLETypesTests: XCTestCase {

  // MARK: - Section name

  func testGenerationDerivation_gen4ServiceUUID() {
    let gen4ServiceUUID = CBUUID(string: "61080001-8d6d-82b8-614a-1c8cb0f8dcc6")
    let generation = CoreBluetoothBLETransport.generation(from: [gen4ServiceUUID])
    XCTAssertEqual(generation, "4.0", "61080001 service UUID should derive generation 4.0")
  }
}
```

**Key conventions:**
- `final class` for all test classes
- `// MARK: -` sections group related cases within a file
- Every assertion includes a failure message string (third argument to `XCTAssertEqual`, etc.)
- `func test_{thing}_{condition}()` naming pattern with underscores for readability

**MARK sections** are used consistently; see `GooseBLETypesTests.swift` and `GooseUploadServiceTests.swift`.

## Mocking

**Mock types (in `GooseSwiftTests/`):**

`MockRustBridge` — implements `GooseRustBridging` protocol:
```swift
final class MockRustBridge: GooseRustBridging {
  var lastMethod: String?
  var lastArgs: [String: Any] = [:]
  var stubbedResult: [String: Any] = [:]
  var shouldThrow = false

  func request(method: String, args: [String: Any]) throws -> [String: Any] {
    guard !shouldThrow else { throw MockError.forced }
    lastMethod = method
    lastArgs = args
    return stubbedResult
  }
}
```
- `MockBLEClient.swift` — stub for `CoreBluetoothBLETransport`
- `MockHealthStore.swift` — delegates to `MockRustBridge`; used for `HealthDataStore` tests

**URL mocking for network tests:**
```swift
private final class MockURLProtocol: URLProtocol {
  static var handler: ((URLRequest) -> (HTTPURLResponse, Data?))?
  // ...
}
// Configure via URLSessionConfiguration.ephemeral with protocolClasses
```
Used in `GooseUploadServiceTests.swift` for HTTP retry logic.

**What to mock:**
- `GooseRustBridging` protocol when testing Swift logic that calls the bridge
- `URLProtocol` subclass for HTTP layer tests
- `CoreBluetoothBLETransport` for callers that depend on BLE state

**What NOT to mock:**
- The real `GooseRustBridge` when testing bridge method routing (use actual Rust bridge with a temp DB)
- SQLite — Rust tests use `GooseStore::open_in_memory()` directly

## Rust Test Patterns

**In-memory store (preferred for Rust tests):**
```rust
let store = GooseStore::open_in_memory().unwrap();
store.migrate().unwrap();
```
Note: `open_for_testing()` does not exist. Always use `open_in_memory()`. File-backed temp stores only when testing file-specific behaviour.

**Integration test structure:**
```rust
#[test]
fn parses_hand_derived_goose_v5_get_hello_frame() {
    let parsed = parse_frame_hex(DeviceType::Goose, GET_HELLO_FRAME).unwrap();
    assert_eq!(parsed.raw_len, 16);
    assert!(parsed.header_crc_valid);
}
```

**Bridge JSON tests:**
```rust
#[test]
fn bridge_returns_core_version_payload() {
    let response = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "version-1",
        "method": "core.version",
        "args": {}
    }));
    // assert response fields
}
```

**Fixture data:**
- Hex frame constants defined as `const` at the top of each test file
- `build_fixture_index()` / `import_fixture_index()` for larger fixture sets

## Swift Test Types and Coverage

**What is tested:**

| File | Coverage area |
|------|--------------|
| `GooseBLETypesTests.swift` | BLE UUID → generation derivation, `rustDeviceType` property, `WearableDescriptor` |
| `GooseUploadServiceTests.swift` | `buildUploadPayload` payload fields, HTTP retry logic (503/200), source-level assertion against hardcoded literals |
| `HRMonitorStateTests.swift` | `CoreBluetoothBLETransport` default state, `@Published` property assignment |
| `TrendsFetchTests.swift` | `HealthDataStore.fetchTrendsSeries` bridge method routing |
| `WorkoutEntryTests.swift` | Workout data model |
| `WorkoutLiveActivityAttributesTests.swift` | `ActivityAttributes` shared type |
| `CoachProviderTests.swift`, `ClaudeProviderTests.swift`, `GeminiProviderTests.swift` | AI coach provider logic |
| `CoachProviderRegistryTests.swift` | Provider registry |
| `CoachKeychainTests.swift` | Keychain read/write |
| `HistoricalRangeParsingTests.swift` | Historical date range parsing |
| `BaselineProgressTests.swift` | Baseline progress computation |
| `TemperatureFormattingTests.swift` | Temperature unit formatting |
| `CustomEndpointProviderTests.swift` | Custom server endpoint validation |
| `WearableDescriptorTests.swift` | Descriptor matching |

**What is NOT tested in Swift:**

- `GooseAppModel` — cannot be instantiated in unit tests (requires BLE hardware and `@MainActor`); source-level assertions used as workaround (see `GooseUploadServiceTests`)
- `GooseRustBridge` FFI layer — covered by Rust tests; not duplicated in Swift
- UI views — no XCTest UI tests; simulator-driven testing is manual
- `OvernightSQLiteMirrorQueue`, `CaptureFrameWriteQueue` — not unit-tested
- `WhoopDataSignalPipeline`, `PassiveActivityDetectionPipeline` — not unit-tested
- `WorkoutLiveActivityController` — no tests

## Rust Test Coverage

**Rust integration test files (44 total in `Rust/core/tests/`):**

| File | Domain |
|------|--------|
| `protocol_tests.rs` | Frame parsing, CRC, payload decoding |
| `bridge_tests.rs` | JSON bridge routing, all bridge methods |
| `store_tests.rs` | SQLite schema migrations, CRUD operations |
| `metrics_tests.rs` | Metric algorithms |
| `metric_features_tests.rs` | Feature extraction from decoded frames |
| `export_tests.rs` | ZIP export, SHA-256 checksums |
| `capture_import_tests.rs` | BLE frame import |
| `capture_correlation_tests.rs` | Frame correlation logic |
| `capture_sanitize_tests.rs` | Input sanitization |
| `sleep_validation_tests.rs` | Sleep scoring |
| `energy_rollup_tests.rs` | Energy/calorie estimation |
| `history_sync_tests.rs` | Historical sync |
| `health_sync_tests.rs` | HealthKit boundary |
| `step_counter_tests.rs`, `step_motion_estimator_tests.rs` | Step counting |
| `exercise_detection_tests.rs` | Auto-detection of workouts |
| `activity_identity_tests.rs`, `activity_candidates_tests.rs` | Activity session logic |
| `timeline_tests.rs` | Timeline queries |
| `calibration_tests.rs` | Calibration algorithms |
| `algorithm_compare_tests.rs`, `reference_tests.rs` | Algorithm vs. Python reference |
| `property_tests.rs` | Property-based tests |
| `perf_budget_tests.rs` | Performance regression tests |
| `privacy_lint_tests.rs` | Privacy constraint checks |
| `debug_ws_tests.rs` | WebSocket debug server |
| `command_tests.rs` | BLE command framing |
| `fixture_tests.rs` | Fixture loading |
| `v24_biometric_bridge_tests.rs`, `v24_biometric_protocol_tests.rs` | v24 biometric packets |
| `heart_rate_gatt_protocol_tests.rs` | HR GATT protocol |
| CLI test files (`*_cli_tests.rs`) | CLI tool integration |

## Async Testing (Swift)

**Pattern for async test methods:**
```swift
func test_upload503_leavesSynced0() async throws {
    // ...
    try? await Task.sleep(nanoseconds: 8_000_000_000)
    XCTAssertEqual(MockURLProtocol.requestCount, 3, "...")
}
```
- `async throws` test methods used when testing `async` upload/network flows
- `XCTSkip` thrown when test preconditions cannot be met (e.g. empty temp DB, sandboxed CI)

## Test Coverage Gaps

**High priority:**
- `GooseAppModel` — all coordinator logic untested at unit level; relies on integration/manual testing
- `OvernightSQLiteMirrorQueue` — overnight guard logic has no automated tests
- `PassiveActivityDetectionPipeline` — heuristic detection logic untested in Swift

**Medium priority:**
- `CoreBluetoothBLETransport` parsing and command logic — only default-state properties tested; GATT protocol parsing untested from Swift
- `CaptureFrameWriteQueue` — batched insert logic untested

**Low priority (covered by Rust):**
- Bridge method routing — thoroughly covered in `Rust/core/tests/bridge_tests.rs`
- Protocol frame parsing — thoroughly covered in `Rust/core/tests/protocol_tests.rs`

---

*Testing analysis: 2026-06-13*
