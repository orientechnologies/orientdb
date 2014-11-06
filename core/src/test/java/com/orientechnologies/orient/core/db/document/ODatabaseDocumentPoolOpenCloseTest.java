package com.orientechnologies.orient.core.db.document;

import static org.testng.AssertJUnit.assertNull;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;

public class ODatabaseDocumentPoolOpenCloseTest {

  @Test
  public void openCloseClearThreadLocal() {
    String url = "memory:" + ODatabaseDocumentPoolOpenCloseTest.class.getSimpleName();
    ODatabaseDocument dbo = new ODatabaseDocumentTx(url).create();
    OPartitionedDatabasePool pool = new OPartitionedDatabasePool(url, "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.close();
    assertNull(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined());
    dbo.drop();
  }

}
