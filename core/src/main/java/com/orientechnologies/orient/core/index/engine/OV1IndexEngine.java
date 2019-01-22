package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;

public interface OV1IndexEngine extends OBaseIndexEngine {
  int VERSION = 1;

  void put(Object key, ORID value);

  @Override
  default int getEngineAPIVersion() {
    return VERSION;
  }

  void load(String indexName, String encryptionKey);
}
