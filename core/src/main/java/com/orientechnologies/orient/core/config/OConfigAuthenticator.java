package com.orientechnologies.orient.core.config;

public class OConfigAuthenticator {
  private boolean enabled;
  private boolean caseSensitive;
  private boolean debug;
  private String name;
  private String type;

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }
}
