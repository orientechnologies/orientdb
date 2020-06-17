package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.util.stream.Stream;

public interface OV1IndexEngine extends OBaseIndexEngine {
  int API_VERSION = 1;

  void put(OAtomicOperation atomicOperation, Object key, ORID value);

  Stream<ORID> get(Object key);

  @Override
  default int getEngineAPIVersion() {
    return API_VERSION;
  }

  void load(
      final String name,
      final int keySize,
      final OType[] keyTypes,
      final OBinarySerializer keySerializer,
      final OEncryption encryption);

  boolean isMultiValue();
}
