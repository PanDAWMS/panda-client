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

    # warn if the selected interpreter's version does not match what PANDA_PYTHONPATH
    # was actually installed for. This commonly happens when an ATLAS release (asetup)
    # is sourced AFTER panda_setup.sh: $VIRTUAL_ENV then points at a different Python
    # than the one panda-client's dependencies were installed for, and the resulting
    # failure is a SyntaxError from deep inside a dependency (e.g. anyio) with no hint
    # of the real cause. Non-fatal -- best-effort detection, not a hard requirement --
    # so it cannot break a setup that happens to work despite the mismatch.
    if [[ -n "$PANDA_PYTHONPATH" ]]; then
        pyver_installed=$(basename "$(dirname "$PANDA_PYTHONPATH")" 2>/dev/null)
        pyver_selected=$("$PANDA_PYTHON_EXEC" -c 'import sys; print("python{}.{}".format(*sys.version_info))' 2>/dev/null)
        if [[ "$pyver_installed" == python* && -n "$pyver_selected" && "$pyver_installed" != "$pyver_selected" ]]; then
            echo "WARNING: panda-client's python packages were installed for ${pyver_installed}, but the selected interpreter"
            echo "         (\$PANDA_PYTHON_EXEC=$PANDA_PYTHON_EXEC) is ${pyver_selected}. This usually means an ATLAS release"
            echo "         (asetup) was sourced after panda_setup.sh, changing \$VIRTUAL_ENV to a different python."
            echo "         If the command below fails with an import error, either re-source panda_setup.sh after asetup,"
            echo "         or set \$PANDA_PYTHON_EXEC to a ${pyver_installed} interpreter explicitly."
        fi
    fi

    # execute
    local exec_string=$1
    shift
    $PANDA_PYTHON_EXEC -u -W ignore -c "${exec_string}" "$@"
}