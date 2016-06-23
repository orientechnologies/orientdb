package com.orientechnologies.common.collection.closabledictionary;

public interface OClosableItem {
  boolean isOpen();
  void close();
  void open();
}
