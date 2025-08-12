package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.text.CollationKey;
import java.text.Collator;

public class StringKeyNormalizer implements KeyNormalizers {
  private final Collator instance = Collator.getInstance();

  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    instance.setDecomposition(decomposition);
    final CollationKey collationKey = instance.getCollationKey((String) key);
    final byte[] ckArray = collationKey.toByteArray();
    final byte[] result = new byte[ckArray.length + 1];
    result[0] = 0;
    System.arraycopy(ckArray, 0, result, 1, ckArray.length);
    return result;
  }
}
