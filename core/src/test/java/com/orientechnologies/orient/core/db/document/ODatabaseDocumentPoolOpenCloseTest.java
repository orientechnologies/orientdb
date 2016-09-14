package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ODatabaseDocumentPoolOpenCloseTest {

  private ODatabaseDocument dbo;

  @Before
  public void setUp() throws Exception {
    String url = "memory:" + ODatabaseDocumentPoolOpenCloseTest.class.getSimpleName();
    dbo = new ODatabaseDocumentTx(url).create();

  }

  @After
  public void tearDown() throws Exception {
    dbo.activateOnCurrentThread();
    dbo.drop();

  }

  @Test
  public void openCloseClearThreadLocal() {
    OPartitionedDatabasePool pool = new OPartitionedDatabasePool(dbo.getURL(), "admin", "admin");
    try {
      ODatabaseDocument db = pool.acquire();
      db.close();
      assertNull(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined());
    } finally {
      pool.close();

    }
  }

  @Test(expected = ODatabaseException.class)
  public void failureOpenPoolDatabase() {

    OPartitionedDatabasePool pool = new OPartitionedDatabasePool(dbo.getURL(), "admin", "admin");
    try {
      ODatabaseDocument db = pool.acquire();
      db.open("admin", "admin");
    } finally {
      pool.close();

    }

  }

  @Test
  public void checkSchemaRefresh() throws ExecutionException, InterruptedException {
    final OPartitionedDatabasePool pool = new OPartitionedDatabasePool(dbo.getURL(), "admin", "admin");
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

    }
  }

}
