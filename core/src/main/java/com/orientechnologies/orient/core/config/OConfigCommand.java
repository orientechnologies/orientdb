package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Map;

@JsonInclude(Include.NON_NULL)
public class OConfigCommand {

  private String name;
  private String pattern;
  private Map<String, String> parameters;

  public OConfigCommand(String name, String pattern, Map<String, String> parameters) {
    super();
    this.name = name;
    this.pattern = pattern;
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }
}
