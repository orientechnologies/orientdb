/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

public class OCommandExecutorSQLUpdateUpsertTest {
  @Test
  public void testUpsert() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateUpsertTest");
    db.create();

    db.command(new OCommandSQL("CREATE class Test")).execute();
    db.command(new OCommandSQL("CREATE property Test.name STRING")).execute();
    db.command(new OCommandSQL("CREATE index Test.name on Test (name) UNIQUE")).execute();

    UpsertThread thread1 = new UpsertThread();
    UpsertThread thread2 = new UpsertThread();
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    List<?> result = db.query(new OSQLSynchQuery<Object>("select from Test"));
    Assert.assertEquals(result.size(), 1);
    ODocument doc = (ODocument) result.get(0);
    Assert.assertEquals(doc.field("name"), "foo");
    db.drop();
  }

  class UpsertThread extends Thread {
    @Override
    public void run() {
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OCommandExecutorSQLUpdateUpsertTest");
      db.open("admin", "admin");
      int sleep = 3000 + new Random().nextInt(2000);
      db.command(new OCommandScript(
          "BEGIN;" + "UPDATE Test SET name = 'foo' UPSERT WHERE name = 'foo';" + "SLEEP " + sleep + ";" + "COMMIT RETRY 10;"))
          .execute();
      db.close();
    }

  }

}
