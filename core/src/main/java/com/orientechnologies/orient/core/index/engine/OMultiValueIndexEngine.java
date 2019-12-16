package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

import java.util.Collection;

public interface OMultiValueIndexEngine extends OV1IndexEngine {
  boolean remove(OAtomicOperation atomicOperation, Object key, ORID value);

  Collection<ORID> get(Object key);
}
