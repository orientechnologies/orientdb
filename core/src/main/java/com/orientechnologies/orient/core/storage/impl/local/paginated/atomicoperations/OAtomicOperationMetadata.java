package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import java.io.Serializable;

public interface OAtomicOperationMetadata<T> extends Serializable {
  String getKey();

  T getValue();
}
