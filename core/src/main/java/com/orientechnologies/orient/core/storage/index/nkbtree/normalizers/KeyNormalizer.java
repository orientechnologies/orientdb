package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyNormalizer {
  private final Map<OType, KeyNormalizers> normalizers = new HashMap<>();

  public KeyNormalizer() {
    normalizers.put(null, new NullKeyNormalizer());
    normalizers.put(OType.INTEGER, new IntegerKeyNormalizer());
    normalizers.put(OType.FLOAT, new FloatKeyNormalizer());
    normalizers.put(OType.DOUBLE, new DoubleKeyNormalizer());
    normalizers.put(OType.SHORT, new ShortKeyNormalizer());
    normalizers.put(OType.BOOLEAN, new BooleanKeyNormalizer());
    normalizers.put(OType.BYTE, new ByteKeyNormalizer());
    normalizers.put(OType.LONG, new LongKeyNormalizer());
    normalizers.put(OType.STRING, new StringKeyNormalizer());
    normalizers.put(OType.DECIMAL, new DecimalKeyNormalizer());
    normalizers.put(OType.DATE, new DateKeyNormalizer());
    normalizers.put(OType.DATETIME, new DateTimeKeyNormalizer());
    normalizers.put(OType.BINARY, new BinaryKeyNormalizer());
  }

  public byte[] normalize(
      final OCompositeKey keys, final OType[] keyTypes, final int decompositon) {
    if (keys == null) {
      throw new IllegalArgumentException("Keys must not be null.");
    }
    if (keys.getKeys().size() != keyTypes.length) {
      throw new IllegalArgumentException(
          "Number of keys must fit to number of types: "
              + keys.getKeys().size()
              + " != "
              + keyTypes.length
              + ".");
    }
    final AtomicInteger counter = new AtomicInteger(0);
    return keys.getKeys().stream()
        .collect(
            ByteArrayOutputStream::new,
            (baos, key) -> {
              normalizeCompositeKeys(baos, key, keyTypes[counter.getAndIncrement()], decompositon);
            },
            (baos1, baos2) -> baos1.write(baos2.toByteArray(), 0, baos2.size()))
        .toByteArray();
  }

  private void normalizeCompositeKeys(
      final ByteArrayOutputStream normalizedKeyStream,
      final Object key,
      final OType keyType,
      final int decompositon) {
    try {
      final KeyNormalizers keyNormalizer = normalizers.get(keyType);
      if (keyNormalizer == null) {
        throw new UnsupportedOperationException(
            "Type " + key.getClass().getTypeName() + " is currently not supported");
      }
      normalizedKeyStream.write(keyNormalizer.execute(key, decompositon));
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }
}
