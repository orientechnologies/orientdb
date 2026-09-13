package com.orientechnologies.tinkerpop.handler;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public class OGraphServerHandlerConfig {

  private boolean enabled = true;
  private Integer graphPoolMax = null;

  public static OGraphServerHandlerConfig fromParameters(OServerParameterConfiguration[] iParams) {
    OGraphServerHandlerConfig config = new OGraphServerHandlerConfig();
    for (OServerParameterConfiguration param : iParams) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        config.enabled = Boolean.parseBoolean(param.getValue());

      } else if (param.getName().equalsIgnoreCase("graph.pool.max"))
        config.graphPoolMax = Integer.parseInt(param.getValue());
    }
    return config;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Integer getGraphPoolMax() {
    return graphPoolMax;
  }
}
