package com.orientechnologies.orient.core.security;

public class OGlobalUserImpl implements OGlobalUser {
  private String name;
  private String password;
  private String resources;

  public OGlobalUserImpl(String name, String password, String resources) {
    this.name = name;
    this.password = password;
    this.resources = resources;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getResources() {
    return resources;
  }

  public void setResources(String resources) {
    this.resources = resources;
  }
}
