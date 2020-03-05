package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class StringKeyNormalizer implements KeyNormalizers {
  final Collator instance = Collator.getInstance();

  @Override
  public byte[] execute(final Object key, final ByteOrder byteOrder, final int decompositon) throws IOException {
    instance.setDecomposition(decompositon);
    final CollationKey collationKey = instance.getCollationKey((String) key);
    final ByteBuffer bb = ByteBuffer.allocate(1);
    bb.put((byte)0);
    return ArrayUtils.addAll(bb.array(), collationKey.toByteArray());
  }
}
