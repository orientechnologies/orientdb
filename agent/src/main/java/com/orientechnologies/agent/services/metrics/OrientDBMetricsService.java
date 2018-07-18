package com.orientechnologies.agent.services.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.OMetricsRegistryFactory;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.agent.services.metrics.server.OrientDBServerMetrics;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.enterprise.server.OEnterpriseServer;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by Enrico Risa on 13/07/2018.
 */
public class OrientDBMetricsService implements OEnterpriseService {

  OEnterpriseServer       server;
  OrientDBMetricsSettings settings;
  OMetricsRegistry        registry;

  OrientDBMetricSupport metricSupport = new OrientDBMetricSupport();

  public OrientDBMetricsService() {

  }

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;
    String configFile = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/profiler.json");
    this.settings = loadConfig(configFile);
    registry = OMetricsRegistryFactory.createProfilerFor(server, this.settings);

  }

  @Override
  public void start() {

    if (settings.enabled) {

      if (settings.server.enabled) {
        metricSupport.add(new OrientDBServerMetrics(server, registry));
      }

      metricSupport.start();
    }
  }

  @Override
  public void stop() {
    metricSupport.stop();
  }

  // "${ORIENTDB_HOME}/config/profiler.json"
  private OrientDBMetricsSettings loadConfig(final String cfgPath) {
    OrientDBMetricsSettings settings = null;

    try {
      if (cfgPath != null) {
        // Default
        String jsonFile = OSystemVariableResolver.resolveSystemVariables(cfgPath);

        File file = new File(jsonFile);

        if (file.exists() && file.canRead()) {
          FileInputStream fis = null;

          try {
            fis = new FileInputStream(file);

            final byte[] buffer = new byte[(int) file.length()];
            fis.read(buffer);

            ObjectMapper mapper = new ObjectMapper();

            settings = mapper.readValue(buffer, OrientDBMetricsSettings.class);

            //            settings = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
          } finally {
            if (fis != null)
              fis.close();
          }
        } else {
          OLogManager.instance()
              .warn(this, "OEnterpriseProfilerFactory.loadConfig() Could not access the security JSON file: %s", null, jsonFile);
        }
      } else {
        OLogManager.instance().warn(this, "OEnterpriseProfilerFactory.loadConfig() Configuration file path is null", null);
      }
    } catch (Exception ex) {
      OLogManager.instance().warn(this, "OEnterpriseProfilerFactory.loadConfig()", ex);
    }

    if (settings == null) {
      OLogManager.instance().warn(this, "The profiler config file was not found. The profiler will be disabled");
      settings = new OrientDBMetricsSettings();
      settings.enabled = false;
    }

    return settings;
  }
}
