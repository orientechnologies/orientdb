/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/** Test distributed TX */
public abstract class AbstractServerClusterTxTest extends AbstractServerClusterInsertTest {
  protected int printBlocksOf = 100;

  protected AbstractServerClusterTxTest() {
    useTransactions = true;
  }

  class TxWriter extends BaseWriter {
    public TxWriter(final int iServerId, final int iThreadId, final ServerRun serverRun) {
      super(iServerId, iThreadId, serverRun);
    }

    @Override
    public Void call() throws Exception {
      final String name = Integer.toString(threadId);
      Set<Integer> clusters = new LinkedHashSet<Integer>();
      LinkedHashMap<String, Long> clusterNames = new LinkedHashMap<String, Long>();
      for (int i = 0; i < count; i++) {
        final ODatabaseDocument database = getDatabase(serverRun);

        try {
          final int id = baseCount + i;

          final String uid = UUID.randomUUID().toString();

          int retry;
          for (retry = 0; retry < maxRetries; retry++) {
            database.activateOnCurrentThread();
            if ((i + 1) % printBlocksOf == 0)
              System.out.println(
                  "\nWriter "
                      + database.getURL()
                      + "(thread="
                      + threadId
                      + ") managed "
                      + (i + 1)
                      + "/"
                      + count
                      + " records so far");

            if (useTransactions) database.begin();

            try {
              ODocument person = createRecord(database, id, uid);
              updateRecord(database, person);
              checkRecord(database, person);
              deleteRecord(database, person);
              checkRecordIsDeleted(database, person);
              person = createRecord(database, id, uid);
              updateRecord(database, person);
              checkRecord(database, person);

              if (useTransactions) database.commit();

              if (delayWriter > 0) Thread.sleep(delayWriter);
              clusters.add(person.getIdentity().getClusterId());

              String clusterName = database.getClusterNameById(person.getIdentity().getClusterId());
              Long counter = clusterNames.get(clusterName);
              if (counter == null) counter = 0L;

              clusterNames.put(clusterName, counter + 1);

              // OK
              break;

            } catch (InterruptedException e) {
              // STOP IT
              System.out.println("Writer received interrupt (db=" + database.getURL());
              Thread.currentThread().interrupt();
              break;
            } catch (ORecordNotFoundException e) {
              // IGNORE IT AND RETRY
              System.out.println(
                  "ORecordNotFoundException Exception caught on writer thread "
                      + threadId
                      + " (db="
                      + database.getURL());
              // e.printStackTrace();
            } catch (ORecordDuplicatedException e) {
              // IGNORE IT AND RETRY
              System.out.println(
                  "ORecordDuplicatedException Exception caught on writer thread "
                      + threadId
                      + " (db="
                      + database.getURL());
              // e.printStackTrace();
            } catch (OTransactionException e) {
              if (e.getCause() instanceof ORecordDuplicatedException)
                // IGNORE IT AND RETRY
                ;
              else throw e;
            } catch (ONeedRetryException e) {
              // System.out.println("ONeedRetryException Exception caught on writer thread " +
              // threadId + " (db=" +
              // database.getURL());

              if (retry >= maxRetries) e.printStackTrace();

            } catch (ODistributedException e) {
              System.out.println(
                  "ODistributedException Exception caught on writer thread "
                      + threadId
                      + " (db="
                      + database.getURL());
              if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                database.rollback();
                throw e;
              }

              // RETRY
            } catch (Throwable e) {
              System.out.println(
                  e.getClass()
                      + " Exception caught on writer thread "
                      + threadId
                      + " (db="
                      + database.getURL());
              e.printStackTrace();
              return null;
            }
          }
        } finally {
          runningWriters.countDown();
          database.activateOnCurrentThread();
          database.close();
        }
      }

      System.out.println(
          "\nWriter "
              + name
              + " END total:"
              + count
              + " clusters:"
              + clusters
              + " names:"
              + clusterNames);
      return null;
    }
  }

  @Override
  protected Callable createWriter(final int i, final int threadId, final ServerRun serverRun) {
    return new TxWriter(i, threadId, serverRun);
  }
}
