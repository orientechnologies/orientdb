package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface OSingleValueIndexEngine extends OV1IndexEngine {
  boolean validatedPut(
      OAtomicOperation atomicOperation,
      Object key,
      ORID value,
      IndexEngineValidator<Object, ORID> validator);

  boolean remove(OAtomicOperation atomicOperation, Object key);

  @Override
  default boolean isMultiValue() {
    return false;
  }
}
