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

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public List<OLdapImportServerListConfig> getServers() {
    return servers;
  }

  public void setServers(List<OLdapImportServerListConfig> servers) {
    this.servers = servers;
  }

  public List<OLdapImportUserConfig> getUsers() {
    return users;
  }

  public void setUsers(List<OLdapImportUserConfig> users) {
    this.users = users;
  }

  public String getAuthenticator() {
    return authenticator;
  }

  public void setAuthenticator(String authenticator) {
    this.authenticator = authenticator;
  }
}
