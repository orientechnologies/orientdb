package com.orientechnologies.orient.core.db.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OLocalBinaryListenersConfigBuilder {

  private List<OLocalBinaryListenerConfig> listeners = new ArrayList<>();

  public OLocalBinaryListenersConfigBuilder addListener(String address, int[] ports) {
    this.listeners.add(new OLocalBinaryListenerConfig(address, ports));
    return this;
  }

  public OLocalBinaryListenersConfig build() {
    return new OLocalBinaryListenersConfig(Collections.unmodifiableList(listeners));
  }
}
