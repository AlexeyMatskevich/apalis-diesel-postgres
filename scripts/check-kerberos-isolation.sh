#!/usr/bin/env bash
# Run the complete database test suite and check for unexpected GSS initialization.
#
# libpq probes for Kerberos credentials on every TCP connection unless
# gssencmode=disable. Kerberos builds that run library finalizers at process
# exit (every macOS build, and MIT krb5 before 1.22 elsewhere) can abort a
# process that exits while another thread is still inside the library. A test
# fixture that builds its own connection string must therefore disable GSS
# itself instead of relying on PGGSSENCMODE from the environment.
#
# MIT Kerberos normally creates KRB5_TRACE when it creates a library context.
# The suite runs without PGGSSENCMODE and with gssencmode=disable appended to
# DATABASE_URL; if the trace file exists afterwards, some test initialized
# Kerberos. A control connection with GSS negotiation enabled first proves
# that psql's libpq and Kerberos libraries produce this signal. Use psql from
# the same native installation as the Rust tests; this is a diagnostic check,
# not a proof of native shutdown correctness.
#
# Usage: DATABASE_URL=postgres://... scripts/check-kerberos-isolation.sh
# DATABASE_URL must be a postgres:// URI for a reachable test database that
# does not set gssencmode itself. This check requires database scenarios and
# always runs every --all-features --tests case serially. Caller arguments
# are rejected because filters and compile/list-only modes change that scope.
set -euo pipefail

if [ "$#" -ne 0 ]; then
  echo "this check accepts no arguments; it always executes the complete test suite" >&2
  exit 2
fi
cd "$(dirname "${BASH_SOURCE[0]}")/.."

database_url="${DATABASE_URL:?DATABASE_URL must point to a reachable test database}"
case "$database_url" in
  postgres://* | postgresql://*) ;;
  *)
    echo "DATABASE_URL must be a postgres:// URI" >&2
    exit 2
    ;;
esac
case "$database_url" in
  *gssencmode=*)
    echo "DATABASE_URL must not set gssencmode; this check sets it" >&2
    exit 2
    ;;
esac
if ! command -v psql > /dev/null; then
  echo "psql is required for the control connection" >&2
  exit 2
fi

trace_dir="$(mktemp -d)"
trap 'rm -rf "$trace_dir"' EXIT

PGGSSENCMODE=prefer KRB5_TRACE="$trace_dir/control" \
  psql "$database_url" --no-psqlrc --quiet --tuples-only --command 'SELECT 1' > /dev/null
if [ ! -e "$trace_dir/control" ]; then
  echo "a GSS-enabled control connection did not initialize Kerberos;" \
    "this libpq and Kerberos combination cannot be checked" >&2
  exit 1
fi

separator='?'
case "$database_url" in
  *\?*) separator='&' ;;
esac
run_cargo() {
  env -u PGGSSENCMODE \
    DATABASE_URL="${database_url}${separator}gssencmode=disable" \
    APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 \
    KRB5_TRACE="$trace_dir/suite" \
    CARGO_TERM_VERBOSE=false CARGO_TERM_QUIET=false RUST_TEST_NOCAPTURE=0 \
    LC_ALL=C \
    cargo test --color never --locked --all-features --tests -- "$@"
}

# Keep target identities: support test names repeat in different binaries.
# Validate harness summaries too, since exit(0) inside a test can make Cargo
# succeed before libtest completes. Empty feature-gated targets are allowed.
test_roster() {
  awk -v phase="$1" '
    /^[[:space:]]*Running (unittests )?[^ ]+\.rs \(/ {
      if (target != "" && !complete) bad = 1
      target = $0
      sub(/^[[:space:]]*Running (unittests )?/, "", target)
      sub(/ \(.*/, "", target)
      count = 0; started = 0; complete = 0
      print "target\t" target
      next
    }
    phase == "list" && /: test$/ {
      if (target == "" || complete) bad = 1
      sub(/: test$/, "")
      print "case\t" target "\t" $0
      count++; total++
    }
    phase == "list" && /^[0-9]+ tests?, [0-9]+ benchmarks?$/ {
      if (target == "" || complete || $1 != count || $3 != 0) bad = 1
      complete = 1
    }
    phase == "run" && /^running [0-9]+ tests?$/ {
      if (target == "" || started || complete || pending != "") bad = 1
      declared = $2; started = 1
    }
    # Output that escapes libtest capture (a native library, a thread without
    # capture) can land between the name of a test and its verdict. The
    # verdict then ends a later line; the name is held until it arrives, and
    # any other report line while it is held is an incomplete report.
    # Only complete libtest report lines end a held name early: a new test
    # line, a summary, or a target header. Diagnostic text may begin with the
    # same words ("running cleanup hook") and is not a report line.
    phase == "run" && pending != "" && (/^test .* \.\.\. / || /^test result: / || /^running [0-9]+ tests?$/) {
      bad = 1
    }
    phase == "run" && pending != "" && /(^|[^[:alnum:]_])ok$/ {
      if (target == "" || !started || complete) bad = 1
      print "case\t" target "\t" pending
      count++; total++; pending = ""
      next
    }
    phase == "run" && /^test .* \.\.\. ok$/ {
      if (target == "" || !started || complete) bad = 1
      sub(/^test /, ""); sub(/ \.\.\. ok$/, "")
      sub(/ - should panic$/, "")
      print "case\t" target "\t" $0
      count++; total++
      next
    }
    # Any verdict other than a complete `ok` is held: a real failure or an
    # ignored test is still rejected, by the harness summary counts and by
    # the roster comparison, whatever text follows the dots.
    phase == "run" && /^test .* \.\.\. / {
      if (target == "" || !started || complete) bad = 1
      sub(/^test /, ""); sub(/ \.\.\. .*$/, "")
      sub(/ - should panic$/, "")
      pending = $0
    }
    phase == "run" && /^test result: ok\./ {
      if (target == "" || !started || complete || pending != "" || $4 != declared || $4 != count ||
          $6 != 0 || $8 != 0 || $10 != 0 || $12 != 0) bad = 1
      complete = 1
    }
    END {
      if (bad || !complete || pending != "" || total == 0) {
        print "incomplete or empty " phase " test report" > "/dev/stderr"
        exit 1
      }
    }
  ' "$2" | LC_ALL=C sort
}

if ! run_cargo --list --format pretty > "$trace_dir/discovery.log" 2>&1; then
  cat "$trace_dir/discovery.log" >&2
  exit 1
fi
if ! test_roster list "$trace_dir/discovery.log" > "$trace_dir/expected"; then
  cat "$trace_dir/discovery.log" >&2
  exit 1
fi
run_cargo --test-threads=1 --format pretty 2>&1 | tee "$trace_dir/execution.log"
test_roster run "$trace_dir/execution.log" > "$trace_dir/executed"
if ! diff -u "$trace_dir/expected" "$trace_dir/executed"; then
  echo "executed tests do not match the complete discovered test suite" >&2
  exit 1
fi

if [ -e "$trace_dir/suite" ]; then
  echo "the test suite initialized Kerberos although DATABASE_URL disables GSS" >&2
  head -n 20 "$trace_dir/suite" >&2
  exit 1
fi
count=$(awk '$1 == "case" { count++ } END { print count + 0 }' "$trace_dir/executed")
echo "checked $count tests against discovery; no Kerberos trace was created"
