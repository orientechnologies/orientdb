package com.orientechnologies.common.factory;

import java.util.LinkedHashMap;
import java.util.Map;

public class ODynamicFactory<K, V> {
  protected final Map<K, V> registry = new LinkedHashMap<K, V>();

  public V get(final K iKey) {
    return registry.get(iKey);
  }

  public void register(final K iKey, final V iValue) {
    registry.put(iKey, iValue);
  }

  public void unregister(final K iKey) {
    registry.remove(iKey);
  }

  public void unregisterAll() {
    registry.clear();
  }
}
