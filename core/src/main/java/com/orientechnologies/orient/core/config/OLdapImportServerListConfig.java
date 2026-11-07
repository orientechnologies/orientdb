package com.orientechnologies.orient.core.config;

public class OLdapImportServerListConfig {

  private String url;
  private boolean isAlias;

  public OLdapImportServerListConfig(String url, boolean isAlias) {
    this.url = url;
    this.isAlias = isAlias;
  }
}
