package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 5/30/14
 */
@Test
public class OpenCloseMTTest {
  private static final int      NUM_THREADS     = 16;
  private static final String   URL             = "plocal:openCloseMTTest";

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private volatile boolean      stop            = false;

  public void openCloseMTTest() throws Exception {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
    OGlobalConfiguration.SECURITY_MAX_CACHED_USERS.setValue(0);
    OGlobalConfiguration.SECURITY_MAX_CACHED_ROLES.setValue(0);

    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(URL);
    databaseDocumentTx.create();
    databaseDocumentTx.close();

    databaseDocumentTx = new ODatabaseDocumentTx(URL);
    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.close();

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < NUM_THREADS; i++)
      futures.add(executorService.submit(new OpenCloser()));

    Thread.sleep(30 * 60 * 1000);

    stop = true;

    for (Future future : futures)
      future.get();

    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.drop();
  }

  public class OpenCloser implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (!stop) {
        try {
          ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(URL);
          databaseDocumentTx.open("admin", "admin");

          List<ODocument> result = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select from OUser"));
          Assert.assertTrue(!result.isEmpty());

          Thread.sleep(500);

          databaseDocumentTx.close();

          Thread.sleep(500);
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
      }
      return null;
    }
  }
}