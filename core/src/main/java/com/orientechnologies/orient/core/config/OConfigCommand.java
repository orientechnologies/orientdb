package com.orientechnologies.orient.core.config;

import java.util.Map;

public class OConfigCommand {

  public OConfigCommand(String name, String patter, Map<String, String> parameters) {
    super();
    this.name = name;
    this.pattern = pattern;
    this.parameters = parameters;
  }

  private String name;
  private String pattern;
  private Map<String, String> parameters;
}
