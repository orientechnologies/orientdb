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

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import junit.framework.Assert;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test distributed TX
 */
public abstract class AbstractServerClusterTxTest extends AbstractServerClusterInsertTest {
  private final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  class TxWriter implements Callable<Void> {
    private final String databaseUrl;
    private int          serverId;

    public TxWriter(final int iServerId, final String db) {
      serverId = iServerId;
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(serverId);
      for (int i = 0; i < count; i++) {
        final ODatabaseDocumentTx database = poolFactory.get(databaseUrl, "admin", "admin").acquire();
        try {
          if ((i + 1) % 100 == 0)
            System.out.println("\nWriter " + database.getURL() + " managed " + (i + 1) + "/" + count + " records so far");

          final int id = baseCount + i;
          database.begin();
          try {
            ODocument person = createRecord(database, serverId, id);
            updateRecord(database, person);
            checkRecord(database, person);
            deleteRecord(database, person);
            checkRecordIsDeleted(database, person);
            // checkIndex(database, (String) person.field("name"), person.getIdentity());

            database.commit();

            Assert.assertTrue(person.getIdentity().isPersistent());
          } catch (Exception e) {
            database.rollback();
            throw e;
          }

          Thread.sleep(delayWriter);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + database.getURL());
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          System.out.println("Writer received exception (db=" + database.getURL());
          e.printStackTrace();
          break;
        } finally {
          runningWriters.countDown();
          database.close();
        }
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }

  }

  protected Callable createWriter(int i, String databaseURL) {
    return new TxWriter(i, databaseURL);
  }

  protected ODocument createRecord(ODatabaseDocumentTx database, int serverId, int i) {
    final int uniqueId = count * serverId + i;

    ODocument person = new ODocument("Person").fields("id", UUID.randomUUID().toString(), "name", "Billy" + uniqueId, "surname",
        "Mayes" + uniqueId, "birthday", new Date(), "children", uniqueId, "serverId", serverId);
    database.save(person);
    return person;
  }

  protected void updateRecord(ODatabaseDocumentTx database, ODocument doc) {
    doc.field("updated", true);
    doc.save();
  }

  protected void checkRecord(ODatabaseDocumentTx database, ODocument doc) {
    doc.reload();
    Assert.assertEquals(doc.field("updated"), Boolean.TRUE);
  }

  protected void deleteRecord(ODatabaseDocumentTx database, ODocument doc) {
    doc.delete();
  }

  protected void checkRecordIsDeleted(ODatabaseDocumentTx database, ODocument doc) {
    final ORecord r = doc.reload();
    Assert.assertNull(r);
  }
}
