package com.orientechnologies.orient.core.metadata.schema;

import java.util.List;
import java.util.Set;

public interface OView extends OClass {
  String getQuery();

  int getUpdateIntervalSeconds();

  List<String> getWatchClasses();

  String getOriginRidField();

  boolean isUpdatable();

  List<String> getNodes();

  List<OViewIndexConfig> getRequiredIndexesInfo();

  String getUpdateStrategy();

  Set<String> getActiveIndexNames();

  long getLastRefreshTime();
}
