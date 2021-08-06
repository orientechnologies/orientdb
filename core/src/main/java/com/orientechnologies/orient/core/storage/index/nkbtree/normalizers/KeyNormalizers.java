package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import java.io.IOException;

public interface KeyNormalizers {
  byte[] execute(Object key, int decomposition) throws IOException;
}
