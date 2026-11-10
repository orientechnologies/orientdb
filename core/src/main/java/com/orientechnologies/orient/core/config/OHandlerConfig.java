package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;

@JsonInclude(Include.NON_NULL)
public class OHandlerConfig {
  private String name;
  private JsonNode config;

  public OHandlerConfig(String name, JsonNode config) {
    this.name = name;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public JsonNode getConfig() {
    return config;
  }

  public void setConfig(JsonNode config) {
    this.config = config;
  }
}
