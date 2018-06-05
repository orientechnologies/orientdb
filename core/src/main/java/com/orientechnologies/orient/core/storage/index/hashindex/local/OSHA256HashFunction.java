package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

public class OSHA256HashFunction<V> implements OHashFunction<V> {
  private final OBinarySerializer<V> valueSerializer;

  public OSHA256HashFunction(OBinarySerializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  @Override
  public long hashCode(V value) {
    final byte[] serializedValue = new byte[valueSerializer.getObjectSize(value)];
    valueSerializer.serializeNativeObject(value, serializedValue, 0);

    final byte[] digest = MessageDigestHolder.instance().get().digest(serializedValue);
    return OLongSerializer.INSTANCE.deserializeNative(digest, 0);
  }
}
