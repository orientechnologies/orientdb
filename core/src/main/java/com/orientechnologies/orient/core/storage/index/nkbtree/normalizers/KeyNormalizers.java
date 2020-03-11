package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;
import java.nio.ByteOrder;

public interface KeyNormalizers {
  byte[] execute(Object key, ByteOrder byteOrder, int decomposition) throws IOException;
}
