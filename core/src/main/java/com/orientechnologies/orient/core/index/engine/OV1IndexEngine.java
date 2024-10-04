package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import java.util.stream.Stream;

public interface OV1IndexEngine extends OBaseIndexEngine {
  int API_VERSION = 1;

  Stream<ORID> get(Object key);

  @Override
  default int getEngineAPIVersion() {
    return API_VERSION;
  }

  boolean isMultiValue();
}
