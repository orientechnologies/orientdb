package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;

public interface OMultiValueIndexEngine extends OV1IndexEngine {
  boolean remove(Object key, ORID value);

  @Override
  default boolean isMultiValue() {
    return true;
  }
}
