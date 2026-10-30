package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OConfigSecurityAuditing {

  private boolean enabled;
  private String engine;
  private JsonNode config;

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void setEngine(String engine) {
    this.engine = engine;
  }

  public void setConfig(JsonNode config) {
    this.config = config;
  }
}
