package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import java.io.Serializable;

/**
 * Basic interface for any kind of metadata which may be stored as part of atomic operation.
 * Java serialization is used to store atomic operation metadata so it is quite slow operation and should be used with care.
 * <p>
 * All metadata are associated with key, if metadata with the same key is put inside of atomic operation previous instance of metadata
 * will be overwritten.
 * <p>
 * To add metadata inside of atomic operation use
 * {@link OAtomicOperation#addMetadata(com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata)}.
 * <p>
 * To read metadata from atomic operation use {@link OAtomicOperation#getMetadata(java.lang.String)}
 * <p>
 * If you wish to read metadata stored inside of atomic operation you may read them from
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord#getAtomicOperationMetadata()}
 *
 * @param <T> Type of atomic operation metadata.
 */
public interface OAtomicOperationMetadata<T> extends Serializable {
  /**
   * @return Key associated with given metadata
   */
  String getKey();

  /**
   * @return Metadata value.
   */
  T getValue();
}
