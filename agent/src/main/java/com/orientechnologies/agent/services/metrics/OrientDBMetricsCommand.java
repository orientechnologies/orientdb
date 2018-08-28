package com.orientechnologies.agent.services.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.OMetric;
import com.orientechnologies.agent.profiler.metrics.OMetricSet;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 06/08/2018.
 */
public class OrientDBMetricsCommand extends OServerCommandAuthenticatedServerAbstract {

  private static final String[] NAMES = { "GET|metrics", "GET|metrics/*", "POST|metrics/config", "POST|metrics/config" };

  private OMetricsRegistry       registry;
  private OrientDBMetricsService service;
  private ObjectMapper           mapper = new ObjectMapper();

  public OrientDBMetricsCommand(OMetricsRegistry registry, OrientDBMetricsService settings) {
    super("server.metrics");
    this.registry = registry;
    this.service = settings;
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: metrics");

    if (iRequest.httpMethod.equalsIgnoreCase("GET")) {
      doGet(iResponse, parts);
    } else if (iRequest.httpMethod.equalsIgnoreCase("POST")) {
      doPost(iRequest, iResponse, parts);
    }

    return false;
  }

  private void doGet(OHttpResponse iResponse, String[] parts) throws IOException {
    if (parts.length == 1) {

      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      registry.toJSON(buffer);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    } else {
      String command = parts[1];

      OrientDBMetricsSettings settings = service.getSettings();
      switch (command) {
      case "prometheus":
        if (settings.reporters.prometheus.enabled) {
          try (StringWriter writer = new StringWriter()) {
            TextFormat.write004(writer, CollectorRegistry.defaultRegistry.filteredMetricFamilySamples(Collections.emptySet()));
            iResponse
                .send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, writer.toString(),
                    null);
          }
        }
        break;
      case "config":
        String valueAsString = mapper.writeValueAsString(settings);
        iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, valueAsString, null);
        break;
      case "list":
        Map<String, String> metrics = getMetricsLists(registry.getMetrics(), "");
        String metricsAsString = mapper.writeValueAsString(metrics);
        iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, metricsAsString, null);
        break;
      }

    }
  }

  private Map<String, String> getMetricsLists(Map<String, OMetric> metrics, String prefix) {

    Map<String, String> m = new HashMap<>();
    metrics.forEach((k, v) -> {
      if (v instanceof OMetricSet) {
        m.putAll(getMetricsLists(((OMetricSet) v).getMetrics(), ((OMetricSet) v).prefix()));
      } else {
        m.put(prefix + "." + k, v.getDescription());
      }
    });
    return m;

  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {
    OrientDBMetricsSettings settings = mapper.readValue(iRequest.content, OrientDBMetricsSettings.class);
    service.changeSettings(settings);
    iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, null, null);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
