package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Created by frank on 29/06/2016.
 */
public class OPartitionedDatabasePoolTest {

  @Rule
  public TestName name = new TestName();

  private ODatabaseDocumentTx      db;
  private OPartitionedDatabasePool pool;

  @Before
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("memory:" + name.getMethodName()).create();
    pool = new OPartitionedDatabasePool(db.getURL(), "admin", "admin");
  }

  @After
  public void tearDown() throws Exception {

    db.activateOnCurrentThread();
    db.drop();

  }

  @Test
  public void shouldAutoCreateDatabase() throws Exception {

    ODatabaseDocumentTx db = pool.acquire();

    assertThat(db.exists()).isTrue();
    assertThat(db.isClosed()).isFalse();
    db.close();

    assertThat(db.isClosed()).isTrue();

    pool.close();

  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIllegalStateWhenAcquireAfterClose() throws Exception {

    pool.close();

    pool.acquire();

  }

  @Test
  public void shouldReturnSameDatabaseOnSameThread() throws Exception {

    ODatabaseDocumentTx db1 = pool.acquire();
    ODatabaseDocumentTx db2 = pool.acquire();

    assertThat(db1).isSameAs(db2);

    db1.close();

    //same instances!!!
    assertThat(db1.isClosed()).isFalse();
    assertThat(db2.isClosed()).isFalse();

    db2.close();
    assertThat(db2.isClosed()).isTrue();

    pool.close();

  }

  @Test
  public void testMultiThread() throws InterruptedException {

    //do a query and assert on other thread
    Runnable acquirer = new Runnable() {
      @Override
      public void run() {

        ODatabaseDocumentTx db = pool.acquire();

        try {
          assertThat(db.isActiveOnCurrentThread()).isTrue();

          List<ODocument> res = db.query(new OSQLSynchQuery<ODocument>("SELECT * FROM OUser"));

          assertThat(res).hasSize(3);

        } finally {

          db.close();
        }

      }

    };

    ExecutorService ex = Executors.newCachedThreadPool();

    for (int i = 0; i < 20; i++) {

      ex.submit(acquirer);
    }

    ex.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void shouldUseEncryption() throws Exception {

    pool.setProperty(STORAGE_ENCRYPTION_METHOD.getKey(), "aes");
    pool.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    ODatabaseDocumentTx dbFromPool = pool.acquire();

    assertThat(dbFromPool.getProperty(STORAGE_ENCRYPTION_METHOD.getKey())).isEqualTo("aes");
    assertThat(dbFromPool.getProperty(STORAGE_ENCRYPTION_KEY.getKey())).isEqualTo("T1JJRU5UREJfSVNfQ09PTA==");

  }

  @Test
  public void shouldBypassSecurity() throws Exception {
    ODatabaseDocumentTx localdb = new ODatabaseDocumentTx("memory:test").create();

    OPartitionedDatabasePool localpool = new OPartitionedDatabasePool("memory:test", "admin", "invalid");
    localpool.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);

    ODatabaseDocumentTx dbFromPool = localpool.acquire();
    dbFromPool.close();

    localpool.close();
  }
}