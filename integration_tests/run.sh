#!/bin/bash

# EDL = earthquake_data_layer
export EDL_ENDPOINT=0.0.0.0
export EDL_PORT=1317
export INTEGRATION_TEST=true

# cd to script directory
original_pwd=$PWD
cd "$(dirname "$0")"

# start container
docker-compose up -d
sleep 5

# run tests
status_code=$(curl --write-out %{http_code} --silent --output /dev/null ${EDL_ENDPOINT}:${EDL_PORT}/collect/sample_id)

# resume original state
docker-compose down
cd $original_pwd

# return result
if [[ "$status_code" -ne 200 ]] ; then
  echo 'failed integration test'
  exit 1
else
  echo 'passed integration test'
  exit 0
fi
