package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by tglman on 30/03/17.
 */
public interface OEmbeddedDatabaseInstanceFactory {

  ODatabaseDocumentEmbedded newInstance(OStorage storage);

  ODatabaseDocumentEmbedded newPoolInstance(ODatabasePoolInternal pool, OStorage storage);
}
