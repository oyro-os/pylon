//! pylon-load — Pusher-protocol load-test harness. See
//! docs/superpowers/specs/2026-06-14-pylon-sp8-load-test-design.md
// The `pusher` module is a library of protocol helpers consumed by later SP8
// tasks (WS client, REST publisher); they are exercised by unit tests now and
// wired into `main` in subsequent tasks, so allow the not-yet-used API.
#![allow(dead_code)]

mod pusher;

fn main() {
    println!("pylon-load");
}
