package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.collection.OLRUCache;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import java.io.UnsupportedEncodingException;

public class OStringCache {

  private final OLRUCache<OStringCacheKey, String> values;

  public OStringCache(int size) {
    values = new OLRUCache<>(size);
  }

  public String getString(final byte[] bytes, final int offset, final int len)
      throws UnsupportedEncodingException {
    OStringCacheKey key = new OStringCacheKey(bytes, offset, len);
    String value;
    synchronized (this) {
      value = this.values.get(key);
    }
    if (value == null) {
      value = new String(bytes, offset, len, HelperClasses.CHARSET_UTF_8).intern();

      // Crate a new buffer to avoid to cache big buffers;
      byte[] newBytes = new byte[len];
      System.arraycopy(bytes, offset, newBytes, 0, len);
      key = new OStringCacheKey(newBytes, 0, len);
      synchronized (this) {
        this.values.put(key, value);
      }
    }
    return value;
  }

  public synchronized void close() {
    values.clear();
  }

  public synchronized int size() {
    return values.size();
  }
}
