package com.orientechnologies.orient.core.config;

public class OConfigProperty {
  private String name;
  private String value;

  public OConfigProperty() {}

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

  public void setName(String name) {
    this.name = name;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
