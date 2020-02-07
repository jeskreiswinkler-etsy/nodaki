#!/bin/bash

export PYSPARK_DRIVER_PYTHON_OPTS='notebook --port=8085 --ip=0.0.0.0'
export PYSPARK_DRIVER_PYTHON=jupyter

function get_jupyter_url {
    jupyter notebook list | tr ' ' '\n' | grep 8085
}

function get_jupyter_url_tail {
    JUPYTER_URL=$1
    echo $JUPYTER_URL | tr ' ' '\n' | grep -o "8085.*"
}

JUPYTER_URL=$(get_jupyter_url)

if [[ -n "${JUPYTER_URL}" ]]; then
    URL_TAIL="$(get_jupyter_url_tail ${JUPYTER_URL})"
    if [[ -n "${URL_TAIL}" ]]; then
        printf "http://%s:%s\n" $(hostname -I) ${URL_TAIL}
        exit
    fi
else
    nohup pyspark > ps.out 2> ps.err < /dev/null &
fi

TRIES=0
MAX_TRIES=10
while true; do
    if [[ "${TRIES}" -gt "${MAX_TRIES}" ]]; then 
        echo "Jupyter instantiation failed; exiting."
    fi
    JUPYTER_URL=$(get_jupyter_url)
    URL_TAIL=$(get_jupyter_url_tail ${JUPYTER_URL})
    if [[ -n "${URL_TAIL}" ]]; then
        printf "http://%s:%s\n" $(hostname -I) ${URL_TAIL}
        break
    fi
    sleep $((2**$TRIES))
    TRIES=$(($TRIES+1))
done
