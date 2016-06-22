package com.orientechnologies.common.collection.closabledictionary;

public class OClosableHolder<V extends OClosableItem> {
  final boolean                      wasOpen;
  private final OClosableEntry<?, V> entry;

  public OClosableHolder(boolean wasOpen, OClosableEntry<?, V> entry) {
    this.wasOpen = wasOpen;
    this.entry = entry;
  }

  public V get() {
    return entry.get();
  }

  OClosableEntry<?, V> getEntry() {
    return entry;
  }
}
