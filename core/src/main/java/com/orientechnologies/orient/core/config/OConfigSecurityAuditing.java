package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OConfigSecurityAuditing {

  private Boolean enabled;
  private String engine;
  private JsonNode config;

  public OConfigSecurityAuditing() {}

  public void setEngine(String engine) {
    this.engine = engine;
  }

  public void setConfig(JsonNode config) {
    this.config = config;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public String getEngine() {
    return engine;
  }

  public JsonNode getConfig() {
    return config;
  }
}
