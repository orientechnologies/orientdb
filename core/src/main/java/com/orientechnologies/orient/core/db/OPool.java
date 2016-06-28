package com.orientechnologies.orient.core.db;

import java.io.Closeable;

/**
 * Created by tglman on 27/06/16.
 */
public interface OPool<T> extends AutoCloseable {

  T acquire();

  void close();
}
