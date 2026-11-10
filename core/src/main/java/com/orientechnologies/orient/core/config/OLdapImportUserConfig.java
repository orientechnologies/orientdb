package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportUserConfig {

  private String baseDN;
  private String filter;
  private List<String> roles;

  public OLdapImportUserConfig() {}

  public OLdapImportUserConfig(String baseDN, String filter, List<String> roles) {
    super();
    this.baseDN = baseDN;
    this.filter = filter;
    this.roles = roles;
  }

  public String getBaseDN() {
    return baseDN;
  }

  public void setBaseDN(String baseDN) {
    this.baseDN = baseDN;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public List<String> getRoles() {
    return roles;
  }

  public void setRoles(List<String> roles) {
    this.roles = roles;
  }
}
