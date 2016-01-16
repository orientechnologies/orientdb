package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.testng.annotations.Test;

import java.util.concurrent.*;

import static org.testng.AssertJUnit.assertNotNull;
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
      assertNull(ODatabaseRecordThreadLocal.instance().getIfDefined());
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


  @Test
  public void checkSchemaRefresh() throws ExecutionException, InterruptedException {
    String url = "memory:" + ODatabaseDocumentPoolOpenCloseTest.class.getSimpleName();
    ODatabaseDocument dbo = new ODatabaseDocumentTx(url).create();
    final OPartitionedDatabasePool pool = new OPartitionedDatabasePool(url, "admin", "admin");
    try {
      ODatabaseDocument db = pool.acquire();
      ExecutorService exec = Executors.newSingleThreadExecutor();
      Future f = exec.submit(new Callable<Object>() {

        @Override
        public Object call() throws Exception {
          ODatabaseDocument db1 = pool.acquire();
          db1.getMetadata().getSchema().createClass("Test");
          db1.close();
          return null;
        }
      });
      f.get();

      exec.shutdown();

      db.activateOnCurrentThread();
      OClass clazz = db.getMetadata().getSchema().getClass("Test");
      assertNotNull(clazz);
      db.close();
    } finally {
      pool.close();
      dbo.activateOnCurrentThread();
      dbo.drop();
    }
  }

}
