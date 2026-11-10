package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

@JsonInclude(Include.NON_NULL)
public class OConfigNetwork {
  private String protocol;
  private String host;
  // TODO: we use to handle ranges ... maybe set ranges.
  private String portRange;
  private OConfigNetworkTls tls;
  private JsonNode parameters;
  private List<OConfigCommand> commands;

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

  public void setCommands(List<OConfigCommand> commands) {
    this.commands = commands;
  }

  public List<OConfigCommand> getCommands() {
    return commands;
  }
}
