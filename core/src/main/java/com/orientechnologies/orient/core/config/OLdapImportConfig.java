package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportConfig {

  private Boolean debug;
  private Boolean enabled;
  private Integer period;
  private List<OLdapImportDatabaseConfig> databases;

  public OLdapImportConfig() {}

  public Boolean getDebug() {
    return debug;
  }

  public void setDebug(Boolean debug) {
    this.debug = debug;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Integer getPeriod() {
    return period;
  }

  public void setPeriod(Integer period) {
    this.period = period;
  }

  public List<OLdapImportDatabaseConfig> getDatabases() {
    return databases;
  }

  public void setDatabases(List<OLdapImportDatabaseConfig> databases) {
    this.databases = databases;
  }
}
