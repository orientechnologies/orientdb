package com.orientechnologies.orient.core.db.config;

import java.util.ArrayList;

public class OUDPUnicastConfigurationBuilder {

  private OUDPUnicastConfiguration confguration = new OUDPUnicastConfiguration();

  public OUDPUnicastConfigurationBuilder setEnabled(boolean enabled) {
    confguration.setEnabled(enabled);
    return this;
  }

  public OUDPUnicastConfigurationBuilder addAddress(String address, int port) {
    confguration.getDiscoveryAddresses().add(new OUDPUnicastConfiguration.Address(address, port));
    return this;
  }

  public OUDPUnicastConfigurationBuilder setPort(int port) {
    confguration.setPort(port);
    return this;
  }

  public OUDPUnicastConfiguration build() {
    OUDPUnicastConfiguration result = new OUDPUnicastConfiguration();
    result.setEnabled(confguration.isEnabled());
    result.setPort(confguration.getPort());
    result.setDiscoveryAddresses(new ArrayList<>(confguration.getDiscoveryAddresses()));
    return result;
  }
}
