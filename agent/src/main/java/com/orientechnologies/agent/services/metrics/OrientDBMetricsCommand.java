package com.orientechnologies.agent.services.metrics;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.Collections;

/**
 * Created by Enrico Risa on 06/08/2018.
 */
public class OrientDBMetricsCommand extends OServerCommandAuthenticatedServerAbstract {

  private static final String[] NAMES = { "GET|metrics", "GET|metrics/*" };

  private OMetricsRegistry        registry;
  private OrientDBMetricsSettings settings;

  public OrientDBMetricsCommand(OMetricsRegistry registry, OrientDBMetricsSettings settings) {
    super("server.metrics");
    this.registry = registry;
    this.settings = settings;
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: metrics");

    if (parts.length == 1) {

      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      registry.toJSON(buffer);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    } else {
      String command = parts[1];
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
      }
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
