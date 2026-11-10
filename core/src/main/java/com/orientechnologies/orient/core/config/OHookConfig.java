package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;

@JsonInclude(Include.NON_NULL)
public class OHookConfig {
  private String name;
  private String legacyClass;
  private String position;
  private JsonNode parameters;

  public OHookConfig() {}

  public void setLagecyClass(String lagecyClass) {
    this.legacyClass = lagecyClass;
  }

  public void setPosition(String position) {
    this.position = position;
  }

  public void setParameters(JsonNode parameters) {
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getLegacyClass() {
    return legacyClass;
  }

  public void setLegacyClass(String legacyClass) {
    this.legacyClass = legacyClass;
  }

  public String getPosition() {
    return position;
  }

  public JsonNode getParameters() {
    return parameters;
  }
}
