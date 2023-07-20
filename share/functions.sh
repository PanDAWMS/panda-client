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

    # execute
    local exec_string=$1
    shift
    $PANDA_PYTHON_EXEC -u -W ignore -c "${exec_string}" "$@"
}