package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public class OJMXPluginConfig {

  private boolean enabled = true;
  private boolean profilerManaged = false;

  public static OJMXPluginConfig fromParamenters(OServerParameterConfiguration[] iParams) {
    OJMXPluginConfig config = new OJMXPluginConfig();
    for (OServerParameterConfiguration param : iParams) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        config.enabled = Boolean.parseBoolean(param.getValue());
      } else if (param.getName().equalsIgnoreCase("profilerManaged"))
        config.profilerManaged = Boolean.parseBoolean(param.getValue());
    }
    return config;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isProfilerManaged() {
    return profilerManaged;
  }

  public void setProfilerManaged(boolean profilerManaged) {
    this.profilerManaged = profilerManaged;
  }
}
