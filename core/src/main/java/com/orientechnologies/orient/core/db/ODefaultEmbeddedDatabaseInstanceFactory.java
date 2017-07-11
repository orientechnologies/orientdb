package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by tglman on 11/07/17.
 */
public class ODefaultEmbeddedDatabaseInstanceFactory implements OEmbeddedDatabaseInstanceFactory {
  @Override
  public ODatabaseDocumentEmbedded newInstance(OStorage storage) {
    return new ODatabaseDocumentEmbedded(storage);
  }

  @Override
  public ODatabaseDocumentEmbedded newPoolInstance(ODatabasePoolInternal pool, OStorage storage) {
    return new OEmbeddedDatabasePool(pool, storage);
  }
}
