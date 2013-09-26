package com.orientechnologies.orient.core.engine.local;

import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OEngineLocalPaginated extends OEngineAbstract {
  public static final String NAME = "plocal";

  public OStorage createStorage(final String dbName, final Map<String, String> configuration) {
    try {
      // GET THE STORAGE
      return new OLocalPaginatedStorage(dbName, dbName, getMode(configuration));

    } catch (Throwable t) {
      OLogManager.instance().error(this,
          "Error on opening database: " + dbName + ". Current location is: " + new java.io.File(".").getAbsolutePath(), t,
          ODatabaseException.class);
    }
    return null;
  }

  public String getName() {
    return NAME;
  }

  public boolean isShared() {
    return true;
  }
}
