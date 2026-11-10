package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportDatabaseConfig {

  private String name;
  private Boolean ignoreLocal;
  private List<OLdapImportDatabaseDomainConfig> domains;

  public OLdapImportDatabaseConfig() {}

  public OLdapImportDatabaseConfig(
      String name, boolean ignoreLocal, List<OLdapImportDatabaseDomainConfig> domains) {
    super();
    this.name = name;
    this.ignoreLocal = ignoreLocal;
    this.domains = domains;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Boolean getIgnoreLocal() {
    return ignoreLocal;
  }

  public void setIgnoreLocal(Boolean ignoreLocal) {
    this.ignoreLocal = ignoreLocal;
  }

  public List<OLdapImportDatabaseDomainConfig> getDomains() {
    return domains;
  }

  public void setDomains(List<OLdapImportDatabaseDomainConfig> domains) {
    this.domains = domains;
  }
}
