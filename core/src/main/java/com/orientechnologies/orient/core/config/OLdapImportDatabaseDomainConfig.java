package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportDatabaseDomainConfig {
  private String domain;
  private List<OLdapImportServerListConfig> servers;
  private List<OLdapImportUserConfig> users;
  private String authenticator;

  public OLdapImportDatabaseDomainConfig(
      String domain,
      List<OLdapImportServerListConfig> servers,
      List<OLdapImportUserConfig> users,
      String authenticator) {
    super();
    this.domain = domain;
    this.servers = servers;
    this.users = users;
    this.authenticator = authenticator;
  }
}
