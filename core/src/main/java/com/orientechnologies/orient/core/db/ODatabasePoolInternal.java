package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.io.Closeable;

/**
 * Created by tglman on 27/06/16.
 */
public interface ODatabasePoolInternal extends AutoCloseable {

  ODatabaseSession acquire();

  void close();

  void release(ODatabaseDocumentInternal database);

  OrientDBConfig getConfig();
}
