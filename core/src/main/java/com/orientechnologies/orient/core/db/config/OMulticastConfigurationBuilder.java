package com.orientechnologies.orient.core.db.config;

public class OMulticastConfigurationBuilder {

  private OMulticastConfguration confguration = new OMulticastConfguration();

  public OMulticastConfigurationBuilder setEnabled(boolean enabled) {
    confguration.setEnabled(enabled);
    return this;
  }

  public OMulticastConfigurationBuilder setDiscoveryPorts(int[] discoveryPorts) {
    confguration.setDiscoveryPorts(discoveryPorts);
    return this;
  }

  public OMulticastConfigurationBuilder setPort(int port) {
    confguration.setPort(port);
    return this;
  }

  public OMulticastConfigurationBuilder setIp(String ip) {
    confguration.setIp(ip);
    return this;
  }

  public OMulticastConfguration build() {
    return new OMulticastConfguration(
        confguration.isEnabled(),
        confguration.getIp(),
        confguration.getPort(),
        confguration.getDiscoveryPorts());
  }
}
