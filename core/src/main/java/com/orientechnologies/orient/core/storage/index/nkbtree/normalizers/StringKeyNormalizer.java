package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StringKeyNormalizer implements KeyNormalizers {
  private final Collator instance = Collator.getInstance();

  @Override
  public byte[] execute(Object key, int decomposition) throws IOException {
    instance.setDecomposition(decomposition);
    final CollationKey collationKey = instance.getCollationKey((String) key);
    final ByteBuffer bb = ByteBuffer.allocate(1);
    bb.put((byte) 0);
    return ArrayUtils.addAll(bb.array(), collationKey.toByteArray());
  }
}
