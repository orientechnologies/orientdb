package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.util.TimerTask;

/**
 * @author Luca Garulli
 */

public class OHashIndexMTSpeedTest {
  private static long                lastCount        = 0;

  private static ODatabaseDocumentTx databaseDocumentTx;
  private static int                 concurrencyLevel = 8;
  private static boolean             useTx            = true;
  private static long                total            = 1000000;

  public static void main(String[] args) throws IOException, InterruptedException {
    OGlobalConfiguration.ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL.setValue(64);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    final String dbUrl = "plocal:" + buildDirectory + "/uniqueHashIndexTest";

    databaseDocumentTx = new ODatabaseDocumentTx(dbUrl);
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }
    databaseDocumentTx.create();

    ODocument metadata = new ODocument().field("partitions", concurrencyLevel * 8);
    final OIndex<?> userIndex = databaseDocumentTx.getMetadata().getIndexManager().createIndex("User.id", "UNIQUE",
        new OSimpleKeyIndexDefinition(-1, OType.LONG), new int[0], null, metadata, "AUTOSHARDING");

    Orient.instance().scheduleTask(new TimerTask() {
      private ODatabaseDocumentTx db = databaseDocumentTx.copy();

      @Override
      public void run() {
        db.activateOnCurrentThread();

        final OIndex<?> index = db.getMetadata().getIndexManager().getIndex("User.id");

        final long count = index.getKeySize();
        System.out.println(String.format("entries=%d %d/sec", count, ((count - lastCount) * 1000 / 2000)));

        lastCount = count;
      }
    }, 2000, 2000);

    final Thread[] threads = new Thread[concurrencyLevel];
    for (int i = 0; i < concurrencyLevel; ++i) {
      final int threadId = i;

      threads[i] = new Thread() {
        @Override
        public void run() {
          final ODatabaseDocumentTx db = databaseDocumentTx.copy();
          db.activateOnCurrentThread();
          db.declareIntent(new OIntentMassiveInsert());

          long totalPerThread = total / concurrencyLevel;
          if (threadId == concurrencyLevel - 1)
            totalPerThread += total % concurrencyLevel;

          final OIndex<?> index = db.getMetadata().getIndexManager().getIndex("User.id");

          if (useTx)
            db.begin();
          for (long k = totalPerThread * threadId; k < totalPerThread * (threadId + 1); ++k) {

            index.put(k, new ORecordId(0, k));

            if (useTx && (k - totalPerThread) % 2 == 0) {
              db.commit();
              db.begin();
            }
          }
          if (useTx)
            db.commit();
        }
      };
    }

    final long beginTime = System.currentTimeMillis();

    for (int i = 0; i < concurrencyLevel; ++i) {
      threads[i].start();
    }

    for (int i = 0; i < concurrencyLevel; ++i) {
      threads[i].join();
    }

    final OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("User.id");
    final long foundKeys = index.getKeySize();

    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.close();

    final long endTime = System.currentTimeMillis();

    System.out.println(String.format("TOTAL TIME: %s AVG %d/sec", OIOUtils.getTimeAsString(endTime - beginTime),
        foundKeys * 1000 / (endTime - beginTime)));
  }
}
