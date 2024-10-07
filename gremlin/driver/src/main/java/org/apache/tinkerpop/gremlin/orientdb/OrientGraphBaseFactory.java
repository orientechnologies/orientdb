package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/** Created by Enrico Risa on 06/09/2017. */
public interface OrientGraphBaseFactory {
  OrientGraph getNoTx();

  OrientGraph getTx();

  default boolean isOpen() {
    return true;
  }

  default void close() {}

  default void drop() {}

  default ODatabaseDocument getDatabase(boolean create, boolean open) {
    return null;
  }
}
