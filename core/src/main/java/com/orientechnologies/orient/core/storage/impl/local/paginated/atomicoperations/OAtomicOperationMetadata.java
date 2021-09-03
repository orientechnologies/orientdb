package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import java.io.Serializable;

/**
 * Basic interface for any kind of metadata which may be stored as part of atomic operation.
 *
 * <p>All metadata are associated with key, if metadata with the same key is put inside of atomic
 * operation previous instance of metadata will be overwritten.
 *
 * <p>To add metadata inside of atomic operation use {@link
 * OAtomicOperationBinaryTracking#addMetadata(com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata)}.
 *
 * <p>To read metadata from atomic operation use {@link
 * OAtomicOperationBinaryTracking#getMetadata(java.lang.String)}
 *
 * <p>If you wish to read metadata stored inside of atomic operation you may read them from {@link
 * com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord#getAtomicOperationMetadata()}
 *
 * <p>If you add new metadata implementation, you have to add custom serialization method in {@link
 * OAtomicUnitEndRecord} class.
 *
 * @param <T> Type of atomic operation metadata.
 */
@SuppressWarnings("SameReturnValue")
public interface OAtomicOperationMetadata<T> extends Serializable {
  /** @return Key associated with given metadata */
  String getKey();

  /** @return Metadata value. */
  T getValue();
}
