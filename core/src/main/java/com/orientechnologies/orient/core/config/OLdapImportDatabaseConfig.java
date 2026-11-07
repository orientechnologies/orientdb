package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportDatabaseConfig {

  private String name;
  private boolean ignoreLocal;
  private List<OLdapImportDatabaseDomainConfig> domains;

  public OLdapImportDatabaseConfig(
      String name, boolean ignoreLocal, List<OLdapImportDatabaseDomainConfig> domains) {
    super();
    this.name = name;
    this.ignoreLocal = ignoreLocal;
    this.domains = domains;
  }
}
