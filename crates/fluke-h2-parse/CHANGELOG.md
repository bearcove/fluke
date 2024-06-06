# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/bearcove/fluke/compare/fluke-h2-parse-v0.1.1...fluke-h2-parse-v0.2.0) - 2024-06-06

### Other
- More Section 8 tests
- Make test invalid_header_block_fragment more readable
- race condition in sends_multiple_window_update_frames_increasing_flow_control_window_above_max_on_stream, we can fix that
- refactor settings sending, add more helpers, etc.
- Introduce SettingsError, fix error code when receiving invalid initial window size
- WIP settings parsing
- 6.5 tests
- 5.1.2 tests
- 5.1 specs compile
- Add rfc 9113 section 5.1 specs
- 4.3 tests
- Clean up 4_1 tests

## [0.1.1](https://github.com/bearcove/fluke/compare/fluke-h2-parse-v0.1.0...fluke-h2-parse-v0.1.1) - 2024-05-27

### Other
- updated the following local packages: fluke-buffet
