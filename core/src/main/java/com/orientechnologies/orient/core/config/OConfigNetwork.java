package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OConfigNetwork {
  private String host;
  // TODO: we use to handle ranges ... maybe set ranges.
  private String portRange;
  private String protocol;
  private OConfigNetworkTls tls;
  private JsonNode parameters;

  // TODO: Add additional protocol settings
  // Traditionally in http could be plugged some more endpoint ... maybe it should be configured
  // here
  // or maybe in the plugin sections
  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPortRange() {
    return portRange;
  }

  public void setPortRange(String portRange) {
    this.portRange = portRange;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public OConfigNetworkTls getTls() {
    return tls;
  }

  public void setTls(OConfigNetworkTls tls) {
    this.tls = tls;
  }

  public JsonNode getParameters() {
    return parameters;
  }

  public void setParameters(JsonNode parameters) {
    this.parameters = parameters;
  }
}
