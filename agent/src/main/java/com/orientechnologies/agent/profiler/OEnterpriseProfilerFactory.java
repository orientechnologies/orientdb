package com.orientechnologies.agent.profiler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OrientDBProfiler;
import com.orientechnologies.common.profiler.OrientDBProfilerStub;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.profiler.ProfilerFactory;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class OEnterpriseProfilerFactory implements ProfilerFactory {
  @Override
  public OrientDBProfiler createProfilerFor(OServer server) {

    String configFile = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/profiler.json");

    String ssf = server.getContextConfiguration().getValueAsString(OGlobalConfiguration.SERVER_PROFILER_FILE);
    if (ssf != null)
      configFile = ssf;
    ODocument config = loadConfig(configFile);

    if (Boolean.TRUE.equals(config.field("enabled"))) {
      return new OrientDBEnterpriseProfiler(config);
    } else {
      return new OrientDBProfilerStub();
    }
  }

  // "${ORIENTDB_HOME}/config/profiler.json"
  private ODocument loadConfig(final String cfgPath) {
    ODocument securityDoc = null;

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

            securityDoc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
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

    if (securityDoc == null) {
      OLogManager.instance().warn(this, "The profiler config file was not found. The profiler will be disabled");
      securityDoc = new ODocument().field("enabled", false);
    }

    return securityDoc;
  }
}
