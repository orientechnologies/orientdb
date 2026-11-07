package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportConfig {

  private boolean debug;
  private boolean enabled;
  private int period;
  private List<OLdapImportDatabaseConfig> databases;

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void setPeriod(int period) {
    this.period = period;
  }
}
