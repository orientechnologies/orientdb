package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public class OCustomSQLFunctionPluginConfig {
  private String config;

  public static OCustomSQLFunctionPluginConfig fromParameters(
      OServerParameterConfiguration[] iParams) {
    OCustomSQLFunctionPluginConfig conf = new OCustomSQLFunctionPluginConfig();
    for (OServerParameterConfiguration param : iParams) {
      if (param.getName().equalsIgnoreCase("config")) {
        conf.config = param.getValue();
      }
    }
    return conf;
  }

  public String getConfig() {
    return config;
  }
}
