package com.orientechnologies.orient.core.db.config;

import java.util.List;

public record OLocalBinaryListenersConfig(List<OLocalBinaryListenerConfig> listeners) {

  public static OLocalBinaryListenersConfigBuilder builder() {
    return new OLocalBinaryListenersConfigBuilder();
  }
}
