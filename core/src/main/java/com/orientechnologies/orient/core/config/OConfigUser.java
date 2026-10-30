package com.orientechnologies.orient.core.config;

import java.util.Optional;

public class OConfigUser {
  private String user;
  private String password;
  private String resources;
  // Maybe have a list of specific rules ....
  private Optional<String> role;

  public OConfigUser() {
    // TODO Auto-generated constructor stub
  }

  public OConfigUser(String user, String password, String resources) {
    this.user = user;
    this.password = password;
    this.resources = resources;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setResources(String resources) {
    this.resources = resources;
  }
}
