package com.orientechnologies.orient.core.metadata.schema;

import java.util.List;

public interface OView extends OClass {
  String getQuery();

  int getUpdateIntervalSeconds();

  List<String> getWatchClasses();

  String getOriginRidField();

  boolean isUpdatable();

  List<String> getNodes();

  List<OViewConfig.OViewIndexConfig> getRequiredIndexesInfo();

  String getUpdateStrategy();
}
