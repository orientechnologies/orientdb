package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;

import java.io.IOException;

public interface OSingleValueIndexEngine extends OV1IndexEngine {
  boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator);

  boolean remove(Object key) throws IOException;

  @Override
  default boolean isMultiValue() {
    return false;
  }
}
