package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.databind.JsonNode;

public class OConfigNetwork {
  private String host;
  // TODO:  we use to handle ranges ... maybe set ranges.
  private int port;
  private String protocol;
  private OConfigNetworkTls tls;
  private JsonNode parameters;
  // TODO: Add additional protocol settings
  // Traditionally in http could be plugged some more endpoint ... maybe it should be configured
  // here
  // or maybe in the plugin sections
}
