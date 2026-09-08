package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OHookConfig {
  private String name;
  private String type;
  private boolean enabled;
  private JsonNode parameters;
}
