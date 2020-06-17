package com.orientechnologies;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.util.Locale;
import org.junit.After;
import org.junit.Before;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/16/14
 */
public abstract class DatabaseAbstractTest {
  protected ODatabaseDocumentTx database;

  public static ENV getEnvironment() {
    String envName = System.getProperty("orientdb.test.env", "dev").toUpperCase(Locale.ENGLISH);
    ENV result = null;
    try {
      result = ENV.valueOf(envName);
    } catch (IllegalArgumentException e) {
    }

    if (result == null) result = ENV.DEV;

    return result;
  }

  public static String getStorageType() {
    if (getEnvironment().equals(ENV.DEV)) return "memory";

    return "plocal";
  }

  @Before
  public void beforeClass() {
    final String dbName = this.getClass().getSimpleName();
    final String storageType = getStorageType();
    final String buildDirectory = System.getProperty("buildDirectory", ".");

    database = new ODatabaseDocumentTx(storageType + ":" + buildDirectory + "/" + dbName);
    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();
  }

  @After
  public void afterClass() throws Exception {
    database.drop();
  }

  public static enum ENV {
    DEV,
    RELEASE,
    CI
  }
}
