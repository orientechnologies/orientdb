package com.orientechnologies.orient.core.config;

public class OLdapImportServerListConfig {

  private String url;
  private Boolean isAlias;

  public OLdapImportServerListConfig() {}

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public Boolean getIsAlias() {
    return isAlias;
  }

  public void setIsAlias(Boolean isAlias) {
    this.isAlias = isAlias;
  }

  public OLdapImportServerListConfig(String url, boolean isAlias) {
    this.url = url;
    this.isAlias = isAlias;
  }
}
