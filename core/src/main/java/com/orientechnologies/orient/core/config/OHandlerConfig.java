package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OHandlerConfig {
  public OHandlerConfig(String name, JsonNode config) {
    this.name = name;
    this.config = config;
  }

  private String name;
  private JsonNode config;
}
