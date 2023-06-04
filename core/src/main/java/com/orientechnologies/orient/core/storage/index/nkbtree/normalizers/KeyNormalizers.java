package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.Collator;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree.BinaryBTree;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;

public class KeyNormalizers {
  private final EnumMap<OType, KeyNormalizer> normalizers = new EnumMap<>(OType.class);

  private final Collator collator;

  public KeyNormalizers(final Locale locale, final int decomposition) {
    normalizers.put(OType.INTEGER, new IntegerKeyNormalizer());
    normalizers.put(OType.FLOAT, new FloatKeyNormalizer());
    normalizers.put(OType.DOUBLE, new DoubleKeyNormalizer());
    normalizers.put(OType.SHORT, new ShortKeyNormalizer());
    normalizers.put(OType.BOOLEAN, new BooleanKeyNormalizer());
    normalizers.put(OType.BYTE, new ByteKeyNormalizer());
    normalizers.put(OType.LONG, new LongKeyNormalizer());
    normalizers.put(OType.DATE, new DateKeyNormalizer());
    normalizers.put(OType.DATETIME, new DateTimeKeyNormalizer());
    normalizers.put(OType.BINARY, new BinaryKeyNormalizer());

    this.collator = Collator.getInstance(locale);
    this.collator.setDecomposition(decomposition);
  }

  public byte[] normalize(final OCompositeKey key, final OType[] keyTypes) {
    if (key == null) {
      throw new IllegalArgumentException("Keys must not be null.");
    }

    final List<Object> keys = key.getKeys();

    if (keys.size() != keyTypes.length) {
      throw new IllegalArgumentException(
              "Number of keys must fit to number of types: "
                      + key.getKeys().size()
                      + " != "
                      + keyTypes.length
                      + ".");
    }

    final ArrayList<byte[]> collatedKeys = new ArrayList<>(keyTypes.length);

    int resultLen = 0;
    for (int i = 0; i < keyTypes.length; i++) {
      final OType type = keyTypes[i];
      final Object pKey = keys.get(i);

      if (pKey == null || pKey instanceof OAlwaysLessKey || pKey instanceof OAlwaysGreaterKey) {
        resultLen += 1;
      } else if (type == OType.STRING) {
        final byte[] collatedKey = collator.getCollationKey((String) pKey).toByteArray();
        resultLen += collatedKey.length + 1;
        collatedKeys.add(collatedKey);
      } else {
        final KeyNormalizer keyNormalizer = normalizers.get(type);

        if (keyNormalizer == null) {
          throw new UnsupportedOperationException("Type " + type + " is currently not supported");
        }

        resultLen += 1;
        resultLen += keyNormalizer.normalizedSize(pKey);
      }
    }

    final byte[] result = new byte[resultLen];
    int offset = 0;
    int keysCursor = 0;

    for (int i = 0; i < keyTypes.length; i++) {
      final OType type = keyTypes[i];
      final Object pKey = keys.get(i);

      if (pKey == null) {
        result[offset] = BinaryBTree.NULL_PREFIX;
        offset++;
      } else if (pKey instanceof OAlwaysLessKey) {
        result[offset] = BinaryBTree.ALWAYS_LESS_PREFIX;
        offset++;
      } else if (pKey instanceof OAlwaysGreaterKey) {
        result[offset] = BinaryBTree.ALWAYS_GREATER_PREFIX;
        offset++;
      } else if (type == OType.STRING) {
        final byte[] collatedKey = collatedKeys.get(keysCursor);
        keysCursor++;

        result[offset] = BinaryBTree.DATA_PREFIX;
        offset++;

        System.arraycopy(collatedKey, 0, result, offset, collatedKey.length);
        offset += collatedKey.length;
      } else {
        result[offset] = BinaryBTree.DATA_PREFIX;
        offset++;

        final KeyNormalizer keyNormalizer = normalizers.get(type);
        offset = keyNormalizer.normalize(pKey, offset, result);
      }
    }

    return result;
  }
}