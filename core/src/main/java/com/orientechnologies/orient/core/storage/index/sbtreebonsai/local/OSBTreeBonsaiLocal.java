package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

import java.io.IOException;
import java.io.PrintStream;

public interface OSBTreeBonsaiLocal<K, V> extends OSBTreeBonsai<K, V> {
  void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer);

  boolean load(OBonsaiBucketPointer rootBucketPointer);

  String getName();

  void rollbackDelete(OAtomicOperation atomicOperation);

  void rollbackPut(byte[] rawKey, byte[] rawValue, OAtomicOperation atomicOperation);

  void rollbackRemove(byte[] rawKey, OAtomicOperation atomicOperation);

  void debugPrintBucket(PrintStream writer) throws IOException;
}
