package com.orientechnologies.security.syslog;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public class ODefaultSyslogConfig {
  private boolean enabled = false;
  private boolean debug = false;
  private String hostname = "localhost";
  private int port = 514;
  private String appName = "OrientDB";

  public static ODefaultSyslogConfig fromParameters(OServerParameterConfiguration[] params) {
    ODefaultSyslogConfig conf = new ODefaultSyslogConfig();
    for (OServerParameterConfiguration param : params) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        conf.enabled = Boolean.parseBoolean(param.getValue());
      } else if (param.getName().equalsIgnoreCase("debug")) {
        conf.debug = Boolean.parseBoolean(param.getValue());
      } else if (param.getName().equalsIgnoreCase("hostname")) {
        conf.hostname = param.getValue();
      } else if (param.getName().equalsIgnoreCase("port")) {
        conf.port = Integer.parseInt(param.getValue());
      } else if (param.getName().equalsIgnoreCase("appName")) {
        conf.appName = param.getValue();
      }
    }
    return conf;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isDebug() {
    return debug;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public String getAppName() {
    return appName;
  }
}
