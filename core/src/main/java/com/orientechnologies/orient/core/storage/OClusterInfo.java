package com.orientechnologies.orient.core.storage;

public interface OClusterInfo {

  String getName();

  String getFileName();

  int getId();

  long getEntries();

  String getRecordConflictStrategyName();

  String encryption();
}
