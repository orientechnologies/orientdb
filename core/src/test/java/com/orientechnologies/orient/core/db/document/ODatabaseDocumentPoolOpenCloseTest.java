package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNull;

public class ODatabaseDocumentPoolOpenCloseTest {

  @Test
  public void openCloseClearThreadLocal() {
    String url = "memory:" + ODatabaseDocumentPoolOpenCloseTest.class.getSimpleName();
    ODatabaseDocument dbo = new ODatabaseDocumentTx(url).create();
    OPartitionedDatabasePool pool = new OPartitionedDatabasePool(url, "admin", "admin");
    try {
      ODatabaseDocument db = pool.acquire();
      db.close();
      assertNull(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined());
    } finally {
      pool.close();

      dbo.activateOnCurrentThread();
      dbo.drop();
    }
  }

  @Test(expectedExceptions = ODatabaseException.class)
  public void failureOpenPoolDatabase() {

    String url = "memory:" + ODatabaseDocumentPoolOpenCloseTest.class.getSimpleName();
    ODatabaseDocument dbo = new ODatabaseDocumentTx(url).create();
    OPartitionedDatabasePool pool = new OPartitionedDatabasePool(url, "admin", "admin");
    try {
      ODatabaseDocument db = pool.acquire();
      db.open("admin", "admin");
    } finally {
      pool.close();

      dbo.activateOnCurrentThread();
      dbo.drop();
    }

  }

}
