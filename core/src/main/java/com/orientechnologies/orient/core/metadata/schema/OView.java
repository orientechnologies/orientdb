package com.orientechnologies.orient.core.metadata.schema;

public interface OView extends OClass {
  String getQuery();

  int getUpdateIntervalSeconds();
}
