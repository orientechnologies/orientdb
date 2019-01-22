package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;

import java.util.Collection;

public interface OMultiValueIndexEngine extends OV1IndexEngine {
  boolean remove(Object key, ORID value);

  Collection<ORID> get(Object key);
}
