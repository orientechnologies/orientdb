package com.orientechnologies.orient.core.config;

import java.util.List;

public class OLdapImportUserConfig {

  private String baseDN;
  private String filter;
  private List<String> roles;

  public OLdapImportUserConfig(String baseDN, String filter, List<String> roles) {
    super();
    this.baseDN = baseDN;
    this.filter = filter;
    this.roles = roles;
  }
}
