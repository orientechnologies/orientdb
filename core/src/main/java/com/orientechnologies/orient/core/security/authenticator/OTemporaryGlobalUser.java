package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.orient.core.security.OGlobalUser;

public class OTemporaryGlobalUser implements OGlobalUser {

  private String name;
  private String password;
  private String resources;

  public OTemporaryGlobalUser(String user, String password, String resources) {
    super();
    this.name = user;
    this.password = password;
    this.resources = resources;
  }

  public String getName() {
    return name;
  }

  public void setName(String user) {
    this.name = user;
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
