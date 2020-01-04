package com.orientechnologies.orient.client.remote;

import java.util.Map;

public class TreeEntry<EK, EV> implements Map.Entry<EK, EV> {
  private final EK key;
  private final EV value;

  public TreeEntry(EK key, EV value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public EK getKey() {
    return key;
  }

  @Override
  public EV getValue() {
    return value;
  }

  @Override
  public EV setValue(EV value) {
    throw new UnsupportedOperationException();
  }
}
