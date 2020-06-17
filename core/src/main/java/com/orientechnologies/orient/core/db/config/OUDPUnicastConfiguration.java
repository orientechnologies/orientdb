package com.orientechnologies.orient.core.db.config;

import java.util.ArrayList;
import java.util.List;

public class OUDPUnicastConfiguration {

  public static class Address {
    private String address;
    private int port;

    public Address(String address, int port) {
      this.address = address;
      this.port = port;
    }

    public String getAddress() {
      return address;
    }

    public int getPort() {
      return port;
    }
  }

  private boolean enabled = true;
  private int port = 4321;
  private List<Address> discoveryAddresses = new ArrayList<>();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public List<Address> getDiscoveryAddresses() {
    return discoveryAddresses;
  }

  public void setDiscoveryAddresses(List<Address> discoveryAddresses) {
    this.discoveryAddresses = discoveryAddresses;
  }

  public static OUDPUnicastConfigurationBuilder builder() {
    return new OUDPUnicastConfigurationBuilder();
  }
}
