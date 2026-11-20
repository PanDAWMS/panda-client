#!/bin/bash
# Smoke-test a relocatable panda-client tarball the way ATLASLocalRootBase
# consumes it: unpack somewhere, replace the /fixMe-PANDACLIENT_BASE
# placeholder with the real path, source panda_setup.sh, run the commands.
#
# Usage: smoke_test_relocatable.sh <tarball> <python-executable>
#
# Run once per bundled Python version: each lib/pythonX.Y tree is only ever
# loaded by its own interpreter, so one version passing says nothing about
# the others.

set -euo pipefail

TARBALL=$(realpath "$1")
PYTHON_EXEC=$(command -v "$2")
PLACEHOLDER=/fixMe-PANDACLIENT_BASE

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# --- unpack and relocate --------------------------------------------------
tar -xzf "$TARBALL" -C "$WORK"
DEST=$(find "$WORK" -mindepth 1 -maxdepth 1 -type d)
{ grep -rlI --null "$PLACEHOLDER" "$DEST" || true; } | xargs -0 -r sed -i "s|$PLACEHOLDER|$DEST|g"
if grep -rlI "$PLACEHOLDER" "$DEST"; then
    echo "FAIL: placeholder still present after relocation in the files above" >&2
    exit 1
fi

# --- set up the environment exactly as users get it -----------------------
export PANDA_PYTHON_EXEC="$PYTHON_EXEC"
export HOME="$WORK/home"
mkdir -p "$HOME"
unset PYTHONPATH VIRTUAL_ENV CONDA_PREFIX
set +eu  # panda_setup.sh is not written for errexit/nounset
# shellcheck disable=SC1091
source "$DEST/etc/panda/panda_setup.sh"
set -eu

PYVER=$("$PANDA_PYTHON_EXEC" -c 'import sys; print("python{}.{}".format(*sys.version_info))')
echo "interpreter:      $PANDA_PYTHON_EXEC ($PYVER)"
echo "PANDA_PYTHONPATH: $PANDA_PYTHONPATH"

# site_path.sh silently falls back to another version's purelib when this
# interpreter's lib/pythonX.Y is missing; that fallback must not hide a
# missing leg here.
if [[ "$PANDA_PYTHONPATH" != "$DEST/lib/$PYVER/site-packages" ]]; then
    echo "FAIL: expected PANDA_PYTHONPATH=$DEST/lib/$PYVER/site-packages" >&2
    exit 1
fi

# --- every bundled package must import ------------------------------------
# Catches a dependency that was installed but whose own dependency did not
# make it into the tarball (the pydantic -> pydantic_core failure in 2.5.0),
# whichever package it is, without having to hit the failing code path.
# -I -S: only the stdlib and the tarball are importable, so a package that
# happens to be installed system-wide on the runner cannot mask a gap.
"$PANDA_PYTHON_EXEC" -I -S - "$PANDA_PYTHONPATH" <<'EOF'
import importlib, os, sys, traceback

site = sys.argv[1]
sys.path.insert(0, site)
names = set()
for entry in os.listdir(site):
    if entry.endswith((".dist-info", ".egg-info", ".pth")) or entry.startswith(("_", ".")):
        continue
    path = os.path.join(site, entry)
    if os.path.isdir(path) and os.path.exists(os.path.join(path, "__init__.py")):
        names.add(entry)
    elif entry.endswith(".py"):
        names.add(entry[:-3])

# modules the bin/ wrappers load (pcontainer is skipped: it runs on import)
names.update(
    "pandaclient." + m
    for m in ("Client", "PBookScript", "PathenaScript", "PchainScript", "PChainScript_native", "PhpoScript", "PrunScript")
)

failed = []
for name in sorted(names):
    try:
        importlib.import_module(name)
        print("  import ok  ", name)
    except Exception:
        print("  import FAIL", name)
        traceback.print_exc()
        failed.append(name)
sys.exit(1 if failed else 0)
EOF

# --- the commands themselves, through bin/ and functions.sh ----------------
run() {
    echo "+ $*"
    "$@" > "$WORK/out.txt" 2>&1 || { cat "$WORK/out.txt"; echo "FAIL: $*" >&2; exit 1; }
}
run pbook --version
run pbook --help
run pbook show --help
run prun --help
run pathena --help
run phpo --help
run pchain --help

echo "smoke test passed for $PYVER"
