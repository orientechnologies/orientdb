package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.util.HashMap;
import java.util.Map;

public class OProxyServerConfig {
  private static final OLogger logger = OLogManager.instance().logger(OProxyServerConfig.class);

  private boolean enabled = true;
  private String remoteHost;
  private Map<Integer, Integer> ports = new HashMap<>();
  private String tracing = "byte";

  public static OProxyServerConfig fromParameters(OServerParameterConfiguration[] params) {
    OProxyServerConfig conf = new OProxyServerConfig();
    for (OServerParameterConfiguration param : params) {
      if (param.getName().equalsIgnoreCase("enabled"))
        conf.enabled = Boolean.parseBoolean(param.getValue());
      else if (param.getName().equalsIgnoreCase("remoteHost")) conf.remoteHost = param.getValue();
      else if (param.getName().equalsIgnoreCase("tracing")) {
        if (!"none".equalsIgnoreCase(param.getValue())
            && !"byte".equalsIgnoreCase(param.getValue())
            && !"hex".equalsIgnoreCase(param.getValue()))
          logger.error("Invalid tracing value: %s", null, param.getValue());
        else {
          conf.tracing = param.getValue();
        }

      } else if (param.getName().equalsIgnoreCase("ports")) {
        String portsAsString = param.getValue();

        final String[] pairs = portsAsString.split(",");
        for (String pair : pairs) {
          final String[] fromTo = pair.split("->");
          if (fromTo.length != 2)
            throw new OConfigurationException(
                "Proxy server: port configuration is not valid. Format: portFrom->portTo");
          conf.ports.put(Integer.parseInt(fromTo[0]), Integer.parseInt(fromTo[1]));
        }
      }
    }
    return conf;
  }

  public static OLogger getLogger() {
    return logger;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getRemoteHost() {
    return remoteHost;
  }

  public Map<Integer, Integer> getPorts() {
    return ports;
  }

  public String getTracing() {
    return tracing;
  }

  public void setPorts(String portsAsString) {
    ports.clear();

    final String[] pairs = portsAsString.split(",");
    for (String pair : pairs) {
      final String[] fromTo = pair.split("->");
      if (fromTo.length != 2)
        throw new OConfigurationException(
            "Proxy server: port configuration is not valid. Format: portFrom->portTo");
      ports.put(Integer.parseInt(fromTo[0]), Integer.parseInt(fromTo[1]));
    }
  }

  public void setTracing(String tracing) {
    this.tracing = tracing;
  }
}
