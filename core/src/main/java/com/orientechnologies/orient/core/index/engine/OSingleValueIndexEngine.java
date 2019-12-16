package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface OSingleValueIndexEngine extends OV1IndexEngine {
  ORID get(Object key);

  boolean validatedPut(Object key, OAtomicOperation atomicOperation, ORID value, Validator<Object, ORID> validator);
}
