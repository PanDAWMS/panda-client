# execute panda-client command
function exec_p_command () {
    export LD_LIBRARY_PATH_ORIG=${LD_LIBRARY_PATH}
    export LD_LIBRARY_PATH=
    export PYTHONPATH_ORIG=${PYTHONPATH}
    export PYTHONPATH=${PANDA_PYTHONPATH}
    export PYTHONHOME_ORIG=${PYTHONHOME}
    unset PYTHONHOME

    # look for option for python3
    for i in "$@"
    do
    case $i in
      -3)
      PANDA_PY3=1
      ;;
      *)
      ;;
    esac
    done

    # check virtual env
    if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
        if [[ -n "$VIRTUAL_ENV" ]]; then
            if [[ -z "$PANDA_PY3" ]]; then
                PANDA_PYTHON_EXEC=${VIRTUAL_ENV}/bin/python
                if [ ! -f "$PANDA_PYTHON_EXEC" ]; then
                    unset PANDA_PYTHON_EXEC
                fi
            fi
            if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
                PANDA_PYTHON_EXEC=${VIRTUAL_ENV}/bin/python3
                if [ ! -f "$PANDA_PYTHON_EXEC" ]; then
                    unset PANDA_PYTHON_EXEC
                fi
            fi
        fi
    fi

    # check conda
    if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
        if [[ -n "$CONDA_PREFIX" ]]; then
            if [[ -z "$PANDA_PY3" ]]; then
                PANDA_PYTHON_EXEC=${CONDA_PREFIX}/bin/python
                if [ ! -f  "$PANDA_PYTHON_EXEC" ]; then
                    unset PANDA_PYTHON_EXEC
                fi
            fi
            if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
                PANDA_PYTHON_EXEC=${CONDA_PREFIX}/bin/python3
                if [ ! -f  "$PANDA_PYTHON_EXEC" ]; then
                    unset PANDA_PYTHON_EXEC
                fi
            fi
        fi
    fi

    # system python
    if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
        if [[ -z "$PANDA_PY3" ]]; then
            PANDA_PYTHON_EXEC=/usr/bin/python
            if [ ! -f  "$PANDA_PYTHON_EXEC" ]; then
                unset PANDA_PYTHON_EXEC
            fi
        fi
        if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
            PANDA_PYTHON_EXEC=/usr/bin/python3
            if [ ! -f  "$PANDA_PYTHON_EXEC" ]; then
                unset PANDA_PYTHON_EXEC
            fi
        fi
    fi

    # no interpreter
    if [[ -z "$PANDA_PYTHON_EXEC" ]]; then
        echo "ERROR: No python interpreter found in \$VIRTUAL_ENV/bin, \$CONDA_PREFIX/bin, or /usr/bin. You may set \$PANDA_PYTHON_EXEC if python is available in another location"
        exit 1
    fi

    # If the selected interpreter's version does not match what PANDA_PYTHONPATH was
    # actually installed for, self-correct rather than just warn. This happens whenever
    # something else in the shell (commonly ATLASLocalRootBase's setupATLAS -3/asetup,
    # which sets PANDA_PYTHON_EXEC as a bare, $PATH-resolved "python3" for ITS OWN
    # bundled panda tooling) overrides the interpreter panda-client would otherwise pick,
    # with no awareness of this install's own, separately-versioned PANDA_PYTHONPATH.
    # Left uncorrected, the failure surfaces as a SyntaxError from deep inside a
    # dependency (e.g. anyio's _lazyimport.py) with no hint of the real cause.
    #
    # The fix only applies when PANDA_SYS/bin/pythonX.Y (the interpreter this specific
    # install's dependencies were actually installed for) exists -- i.e. only when we
    # have a KNOWN-GOOD replacement in hand, not a guess. Otherwise this falls back to
    # a warning, since there is nothing safe to switch to.
    if [[ -n "$PANDA_PYTHONPATH" ]]; then
        pyver_installed=$(basename "$(dirname "$PANDA_PYTHONPATH")" 2>/dev/null)
        pyver_selected=$("$PANDA_PYTHON_EXEC" -c 'import sys; print("python{}.{}".format(*sys.version_info))' 2>/dev/null)
        if [[ "$pyver_installed" == python* && -n "$pyver_selected" && "$pyver_installed" != "$pyver_selected" ]]; then
            known_good_exec="${PANDA_SYS}/bin/${pyver_installed}"
            if [[ -n "$PANDA_SYS" && -x "$known_good_exec" ]]; then
                echo "NOTE: \$PANDA_PYTHON_EXEC ($PANDA_PYTHON_EXEC, ${pyver_selected}) does not match the python"
                echo "      panda-client's dependencies were installed for (${pyver_installed}) -- likely overridden by"
                echo "      something else in the shell (e.g. ATLASLocalRootBase's setupATLAS -3/asetup). Using"
                echo "      ${known_good_exec} instead, which this install actually ships for ${pyver_installed}."
                PANDA_PYTHON_EXEC="$known_good_exec"
            else
                echo "WARNING: panda-client's python packages were installed for ${pyver_installed}, but the selected interpreter"
                echo "         (\$PANDA_PYTHON_EXEC=$PANDA_PYTHON_EXEC) is ${pyver_selected}. This usually means something else in"
                echo "         the shell (e.g. an ATLAS release's asetup) overrode the interpreter panda-client would pick."
                echo "         If the command below fails with an import error, set \$PANDA_PYTHON_EXEC to a ${pyver_installed}"
                echo "         interpreter explicitly -- no ${pyver_installed} interpreter was found under \$PANDA_SYS/bin to"
                echo "         switch to automatically."
            fi
        fi
    fi

    # execute
    local exec_string=$1
    shift
    $PANDA_PYTHON_EXEC -u -W ignore -c "${exec_string}" "$@"
}