package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Optional;

@JsonInclude(Include.NON_NULL)
public class OConfigUser {
  private String user;
  private String password;
  private String resources;
  // Maybe have a list of specific rules ....
  private Optional<String> role;

  public OConfigUser() {}

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

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getResources() {
    return resources;
  }

  public Optional<String> getRole() {
    return role;
  }
}
