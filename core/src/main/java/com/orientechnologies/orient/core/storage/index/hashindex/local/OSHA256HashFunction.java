package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

public class OSHA256HashFunction<V> implements OHashFunction<V> {
  private final OBinarySerializer<V> valueSerializer;

  public OSHA256HashFunction(OBinarySerializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  @Override
  public long hashCode(byte[] value) {
    final byte[] digest = MessageDigestHolder.instance().get().digest(value);
    return OLongSerializer.INSTANCE.deserializeNative(digest, 0);
  }

  @Override
  public long hashCode(V value, Object[] keyTypes) {
    final byte[] serializedValue = new byte[valueSerializer.getObjectSize(value, keyTypes)];
    valueSerializer.serializeNativeObject(value, serializedValue, 0, keyTypes);

    final byte[] digest = MessageDigestHolder.instance().get().digest(serializedValue);
    return OLongSerializer.INSTANCE.deserializeNative(digest, 0);
  }

}
