package com.orientechnologies.agent.services.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.Utils;
import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.OMetricsRegistryFactory;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.agent.services.metrics.server.OrientDBServerMetrics;
import com.orientechnologies.agent.services.metrics.server.database.OrientDBDatabasesMetrics;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * Created by Enrico Risa on 13/07/2018.
 */
public class OrientDBMetricsService implements OEnterpriseService {
  private static String PROFILER_SCHEMA = "ProfilerConfig";

  OEnterpriseServer       server;
  OrientDBMetricsSettings settings;
  OMetricsRegistry        registry;

  OrientDBMetricSupport metricSupport = new OrientDBMetricSupport();

  public OrientDBMetricsService() {

  }

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;

    String jsonConfig = server.getSystemDatabase().executeWithDB((db) -> {
      OSchema schema = db.getMetadata().getSchema();
      if (!schema.existsClass(PROFILER_SCHEMA)) {
        schema.createClass(PROFILER_SCHEMA);
      }

      try (OResultSet resultSet = db.query("select from " + PROFILER_SCHEMA + " limit 1")) {
        OElement element = resultSet.stream().map((r) -> r.getElement().get()).findFirst().orElseGet(() -> {
          String configFile = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/profiler.json");
          String content;
          try {
            content = loadContent(configFile);
          } catch (IOException e) {
            OLogManager.instance()
                .warn(this, "OEnterpriseProfilerFactory.loadConfig() Could not access the profiler JSON file: %s", null,
                    configFile);
            content = "{ \"enabled\" : false}";
          }
          if (content == null) {
            content = "{ \"enabled\" : false}";
          }
          ODocument document = new ODocument(PROFILER_SCHEMA).fromJSON(content);
          return (OElement) db.save(document);
        });

        return element.toJSON();
      }
    });
    this.settings = deserializeConfig(jsonConfig);
    registry = OMetricsRegistryFactory.createRegistryFor(server, this.settings);

    config();

  }

  @Override
  public void start() {

    metricSupport.start();

    server.registerStatelessCommand(new OrientDBMetricsCommand(server, registry, this));

  }

  private void config() {
    if (settings.enabled) {

      if (settings.server.enabled) {
        metricSupport.add(new OrientDBServerMetrics(server, registry));
      }

      if (settings.database.enabled) {
        metricSupport.add(new OrientDBDatabasesMetrics(server, registry));
      }

    }
  }

  @Override
  public void stop() {
    metricSupport.stop();
  }

  // "${ORIENTDB_HOME}/config/profiler.json"

  private String loadContent(final String cfgPath) throws IOException {
    if (cfgPath != null) {
      // Default
      String jsonFile = OSystemVariableResolver.resolveSystemVariables(cfgPath);
      File file = new File(jsonFile);
      if (file.exists() && file.canRead()) {
        return OIOUtils.readFileAsString(file);
      }
    }
    return null;
  }

  private OrientDBMetricsSettings deserializeConfig(String cfg) {

    ObjectMapper mapper = new ObjectMapper();

    try {
      return mapper.readValue(cfg, OrientDBMetricsSettings.class);
    } catch (IOException e) {
      OLogManager.instance().warn(this, "OEnterpriseProfilerFactory.loadConfig() the profiler will be disabled", e);
    }

    OrientDBMetricsSettings settings = new OrientDBMetricsSettings();
    settings.enabled = false;
    return settings;
  }

  public OrientDBMetricsSettings getSettings() {
    return settings;
  }

  public void changeSettings(OrientDBMetricsSettings settings) {
    metricSupport.stop();

    saveSettings(settings);
    this.settings = settings;
    config();

    metricSupport.start();
  }

  private void saveSettings(OrientDBMetricsSettings settings) {

    server.getSystemDatabase().executeInDBScope((db) -> {

      try (OResultSet resultSet = db.query("select from " + PROFILER_SCHEMA + " limit 1")) {
        Optional<OResult> first = resultSet.stream().findFirst();

        if (first.isPresent()) {
          ObjectMapper mapper = new ObjectMapper();
          OElement element = first.get().getElement().get();
          String s = mapper.writeValueAsString(settings);
          element = element.fromJSON(s);
          db.save(element);
        }
      } catch (JsonProcessingException e) {
        OLogManager.instance().warn(this, "Error saving profiler config");
      }

      return null;
    });

  }

  public String toJson() {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try {
      this.registry.toJSON(buffer);
      return buffer.toString();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error " + e.getMessage(), e);
    }
    return null;
  }

  public OMetricsRegistry getRegistry() {
    return registry;
  }
}
