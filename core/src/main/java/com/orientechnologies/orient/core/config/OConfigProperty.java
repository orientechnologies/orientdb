package com.orientechnologies.orient.core.config;

public class OConfigProperty {
  private String name;
  private String value;

  public OConfigProperty(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }
}
