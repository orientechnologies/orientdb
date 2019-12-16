package com.orientechnologies.common.function;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface TxFunction<T> {
  T accept(final OAtomicOperation atomicOperation) throws Exception;
}
