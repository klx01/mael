Just me trying to get familiar with rust by solving the [distributed systems challenges by fly.io](https://fly.io/dist-sys/) challenges.

# Usage
Follow the instructions from https://fly.io/dist-sys/1/ for installing maelstrom and for running the node.

# Workaround over an issue
For some reason, i was not able to build this project in release mode. It was failing with errors like this

    error: failed to run custom build command for `serde_json v1.0.115`

    Caused by:
    process didn't exit successfully: `<path_to_project>/mael/target/release/build/serde_json-fd8c6ac589a2738a/build-script-build` (signal: 9, SIGKILL: kill)

for serde, serde_json and proc-macro2. The debug mode was fine.

After checking some things, i've noticed that any binaries that are built in release mode are getting instantly killed when i run them. So i've checked which options are used in release via

    cargo build -vv -r

and narrowed it down to -C strip=debuginfo . And i've double-checked using the bare rustc, a "hello world" compiled with this option produces a binary that gets killed, and without this option it works fine.

So i've added this to the Cargo.toml 

    [profile.release]
    strip = "none"

And it helped.

As for why this happens, i've only managed to dig this using the log util

    kernel: (AppleSystemPolicy) ASP: Security policy would not allow process: <pid>, <path_to_the_binary_that_ive_tried_to_run>
