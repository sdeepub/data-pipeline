FROM grafana/grafana:10.2.2

# Install Apache IoTDB datasource plugin
RUN grafana cli plugins install apache-iotdb-datasource

# Optional: Pre-create plugins directory (sometimes needed)
RUN mkdir -p /var/lib/grafana/plugins

EXPOSE 3000