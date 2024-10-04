package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import java.util.stream.Stream;

public interface OV1IndexEngine extends OBaseIndexEngine {
  int VERSION = 4;
  int API_VERSION = 1;

  Stream<ORID> get(Object key);

  @Override
  default int getEngineAPIVersion() {
    return API_VERSION;
  }

  boolean isMultiValue();
}
