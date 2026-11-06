package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OHookConfig {
  private String name;
  private String legacyClass;
  private String position;
  private JsonNode parameters;

  public void setLagecyClass(String lagecyClass) {
    this.legacyClass = lagecyClass;
  }

  public void setPosition(String position) {
    this.position = position;
  }

  public void setParameters(JsonNode parameters) {
    this.parameters = parameters;
  }
}
