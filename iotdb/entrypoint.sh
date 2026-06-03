#!/bin/bash
set -e
echo "" >> /iotdb/conf/iotdb-datanode.properties
echo "# REST Service Configuration (added by entrypoint)" >> /iotdb/conf/iotdb-datanode.properties
echo "enable_rest_service=true" >> /iotdb/conf/iotdb-datanode.properties
echo "rest_service_port=8080" >> /iotdb/conf/iotdb-datanode.properties
echo "rest_service_bind_address=0.0.0.0" >> /iotdb/conf/iotdb-datanode.properties
exec /usr/bin/dumb-init -- /iotdb/sbin/start-datanode.sh
