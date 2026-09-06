# `-D warnings` promotes every lint (see Cargo.toml `[lints]`) to an error.
export RUSTFLAGS := env_var_or_default("RUSTFLAGS", "-D warnings")
export RUSTDOCFLAGS := env_var_or_default("RUSTDOCFLAGS", "-D warnings")

_default:
    @just --list

# Full local gate. Run this before saying a change is done.
check: fmt-check clippy test doc

# Format the whole workspace in place.
fmt:
    cargo fmt --all

# Fail if anything is unformatted.
fmt-check:
    cargo fmt --all -- --check

# Lint all targets and features; warnings are errors.
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# Run the full test suite.
test:
    cargo test --all-features

# Property-based tests with more cases than the inline default.
proptest:
    PROPTEST_CASES=2048 cargo test --all-features -- proptest

# Deterministic simulation suite only (seed via SEED=... for a single run).
sim *ARGS:
    cargo test --all-features --test simulation -- {{ARGS}}

# Re-run a failed simulation seed, e.g. `just sim-seed 12345`.
sim-seed SEED:
    SEED={{SEED}} cargo test --all-features --test simulation -- --nocapture

# Build docs the way CI does (broken links are errors).
doc:
    cargo doc --no-deps --all-features

# What CI runs.
ci: fmt-check clippy test doc

# Apply the safe subset of clippy's suggestions.
fix:
    cargo clippy --fix --all-targets --all-features --allow-dirty
