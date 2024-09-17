# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.2](https://github.com/bearcove/loona/compare/loona-v0.3.1...loona-v0.3.2) - 2024-09-17

### Other

- release

## [0.3.1](https://github.com/bearcove/loona/compare/loona-v0.3.0...loona-v0.3.1) - 2024-09-05

### Other
- Update logo attribution
- don't crash on empty header name
- Try to measure servers in steady state
- Filter syscsalls better, introduce record mode for perfstat, remove misleading comment
- use httpwg_harness to run testbed, use h1 there
- both hyper and loona httpwg clis use the harness now
- standardize testing some more
- ignore some failures
- well that's super weird
- try out benchmarks

## [0.3.0](https://github.com/bearcove/loona/compare/loona-v0.2.1...loona-v0.3.0) - 2024-08-21

### Other
- Fix TLS example
- Everything passes
- mh
- so close (for this batch)
- 4
- ahh
- kmn
- ohey error count back to 78?
- 9 errors left
- wip
- more error-enum-ifiying
- [ci skip] logo URL update
- Also add logo to main crate

## [0.2.1](https://github.com/bearcove/loona/compare/loona-v0.2.0...loona-v0.2.1) - 2024-08-14

### Fixed
- Fix rustdoc errors
- Depend on ktls only on Linux

### Other
- Improve httpwg-cli, make it not so noisy
- Merge pull request [#216](https://github.com/bearcove/loona/pull/216) from bearcove/run-doc-in-ci

## [0.1.1](https://github.com/bearcove/fluke/compare/fluke-v0.1.0...fluke-v0.1.1) - 2024-05-27

### Added
- Upgrade dependencies

### Fixed
- fix more tests
- fix several low-hanging bugs: re io/read tasks

### Other
- Try out pre-commit hook
- More testing facilities
- Make rfc9113 pass, still need to check codes
- Alright, back to writing tests!
- Introduce httpwg-macros, httpwg-gen
- Woo, namespacing tests, getting rid of the useless struct. Maybe we can codegen the rest actually.
- Woahey, 4.2 test works?
- More test facilities
- Flesh out the handshake test
- 4.1 test work
- Well, well, almost got a working 4.1 test
- Introduce 4.1 test (needs read loop in the background)
- More test harness generation
- Generate tests
- Introduce start_server function
- Use pipe for httpwg tests
- Migrate ChanRead uses over to pipe
- Finish up pipe implementation
- Simplify WriteOwned trait, introduce pipe
- First test is doing things
- Introduce fluke-h2-parse
- Some tests are starting to _almost_ pass with io-uring-async
- Fix buffer bug
- Remove BufOrSlice, closes [#153](https://github.com/bearcove/fluke/pull/153)
- Bring 'fluke-maybe-uring' back into 'fluke-buffet'
- Do a _bit_ of flow control on incoming streams
- Improve code coverage, closes [#151](https://github.com/bearcove/fluke/pull/151)
- Fix linux build
- Implement final flow control touches
- remove unused methods
- 6.9.1 all three cases pass
- well _I_ think 6.9.1 should pass.
- Headers flow control wip
- wip header flow control
- do flow control accounting for headers as well
- curl tests work! partial writes work.
- wip Piece
- Finish propagating Piece/PieceCore split changes
- Piece::Slice
- drat, partial writes.
- Ready to write
- a little forgetfulness
- BodyEnd sets eof to true
- Remove erroneous/outdated comment (the Frame struct no longer contains the payload)
- Pass h2spec 6.9 tests apparently, even though we're never sending data now?
- Introduce StreamIncoming, StreamOutgoing
- Start implementing flow-control
- hapsoc => fluke
- Fix h2spec http2/6.4/3
- Fix h2spec/6.5.2/1
- Fix h2spec http2/6.5/3
- More h2spec cases
- Close connection explicitly in h2spec
- Upgrade tokio-uring
- mhh
- Fix more cases
- well every time I lose silly time to this I improve debug logging, so.
- mh
- Uhm
- better debug implementation for Frame
- Remove read module, move it back into server.rs
- Remove write module altogether
- Move all writing out of write
- Retire H2ConnEvent
- mhmh
- Move settings acknowledgement inside read
- Deprecate H2ConnEvent::RstStream
- Move goaway writing away from write
- Move ping to write_frame
- Introduce write_frame
- Give out_scratch to h2readcontext
- Get rid of write task
- Fix some flow-control cases
- Fix PING tests
- Fix more tests
- Oh joy, h2spec is wrong
- More test fixes
- Fix more stream state cases
- more stream state things
- Fix some stream state cases
- Rename StreamStage to StreamState
- Implement RstStream
- Fix hpack tests
- looking good
- No more compile errors woo
- Simplifying headers/trailing reading + being more rigorous with conneciton errors
- no continuation state
- Upgrade more deps
- Upgrade pretty-hex
- Upgrade deps
- Graceful GOAWAY handling
- Set minimum rust version for crates with async fn in trait
- Bump some dependencies
- Remove TAIT
- Bump dependencies
- Opt out of async_fn_in_trait warnings
- Move fluke-h2spec somewhere else
- Move curl tests into their own crate
- Simplify H2Result code further, try sccache.exe on Windows
- Have a few methods return H2Result, which clarifies the control flow
- Fix http2/5.1/5
- Introduce 'write' module
- Move types around
- Bring state names closer to RFC
- First stab at 5.1.2
- todo => debug, don't crash on window_update for known streams
- Don't send GO_AWAY when receiving a connection-wide window update
- Send GO_AWAY if we get a stray WINDOW_UPDATE
- Extract process_frame method out of H2ReadContext::work
- release

## [0.1.0](https://github.com/bearcove/fluke/releases/tag/fluke-v0.1.0) - 2023-10-03

### Other

- Fix fluke-maybe-uring cargo-publish showstoppers
- Rebrand to fluke
