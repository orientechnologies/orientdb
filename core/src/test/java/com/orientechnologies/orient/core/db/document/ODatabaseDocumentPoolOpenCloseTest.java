package com.orientechnologies.orient.core.db.document;

import static org.testng.AssertJUnit.assertNull;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;

public class ODatabaseDocumentPoolOpenCloseTest {

  @Test
  public void openCloseClearThreadLocal() {
    String url = "memory:" + ODatabaseDocumentPoolOpenCloseTest.class.getSimpleName();
    ODatabaseDocument dbo = new ODatabaseDocumentTx(url).create();
    ODatabaseDocumentPool pool = new ODatabaseDocumentPool(url, "admin", "admin");
    pool.setup(10, 20);
    ODatabaseDocument db = pool.acquire();
    db.close();
    assertNull(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined());
    pool.close();
    dbo.drop();
  }

}
