/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

import java.util.concurrent.Callable;

/**
 * Test distributed TX
 */
public abstract class AbstractServerClusterTxTest extends AbstractServerClusterInsertTest {
  protected final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  protected AbstractServerClusterTxTest() {
    useTransactions = true;
  }

  class TxWriter extends BaseWriter {
    public TxWriter(final int iServerId, final int iThreadId, final String db) {
      super(iServerId, iThreadId, db);
    }

    @Override
    public Void call() throws Exception {
      final String name = Integer.toString(threadId);

      for (int i = 0; i < count; i++) {
        final ODatabaseDocumentTx database = poolFactory.get(databaseUrl, "admin", "admin").acquire();

        try {
          final int id = baseCount + i;

          int retry = 0;

          for (retry = 0; retry < maxRetries; retry++) {
            if ((i + 1) % 100 == 0)
              System.out.println("\nWriter " + database.getURL() + "(thread=" + threadId + ") managed " + (i + 1) + "/" + count
                  + " records so far");

            if( useTransactions)
              database.begin();

            try {
              ODocument person = createRecord(database, id);
              updateRecord(database, person);
              checkRecord(database, person);
              deleteRecord(database, person);
              checkRecordIsDeleted(database, person);

              person = createRecord(database, id);
              updateRecord(database, person);
              checkRecord(database, person);

              if( useTransactions)
              database.commit();

              if (delayWriter > 0)
                Thread.sleep(delayWriter);

              // OK
              break;

            } catch (InterruptedException e) {
              System.out.println("Writer received interrupt (db=" + database.getURL());
              Thread.currentThread().interrupt();
              break;
            } catch (ORecordDuplicatedException e) {
              // IGNORE IT
            } catch (OTransactionException e) {
              if (e.getCause() instanceof ORecordDuplicatedException)
                // IGNORE IT
                ;
              else
                throw e;
            } catch (ONeedRetryException e) {
              System.out.println("Writer received exception (db=" + database.getURL());

              if (retry >= maxRetries)
                e.printStackTrace();

              break;
            } catch (ODistributedException e) {
              if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                database.rollback();
                throw e;
              }
            } catch (Throwable e) {
              System.out.println("Writer received exception (db=" + database.getURL());
              e.printStackTrace();
              return null;
            }
          }
        } finally {
          runningWriters.countDown();
          database.close();
        }
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }

  }

  @Override
  protected Callable createWriter(int i, final int threadId, String databaseURL) {
    return new TxWriter(i, threadId, databaseURL);
  }
}
