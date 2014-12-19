/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups = "dictionary")
public class TransactionIsolationTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public TransactionIsolationTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testIsolationRepeatableRead() throws IOException {

    ODatabaseDocumentTx db1 = new ODatabaseDocumentTx(url);
    db1.open("admin", "admin");

    ODatabaseDocumentTx db2 = new ODatabaseDocumentTx(url);
    db2.open("admin", "admin");

    ODocument record1 = new ODocument();
    record1.field("name", "This is the first version").save();

    db1.begin();
    try {
      db1.getTransaction().setIsolationLevel(OTransaction.ISOLATION_LEVEL.REPEATABLE_READ);

      // RE-READ THE RECORD
      record1.getIdentity().getRecord();

      // CHANGE THE RECORD FROM DB2
      ODocument record2 = db2.load(record1.getIdentity());
      record2.field("name", "This is the second version").save();

      db1.reload(record1, null, true);

      Assert.assertEquals(record1.field("name"), "This is the first version");
    } catch (IllegalArgumentException e) {
      if (!url.startsWith("remote:"))
        // NOT SUPPORTED IN REMOTE MODE
        Assert.assertFalse(true);
    }
    db1.close();
    db2.close();
  }

  @Test
  public void testIsolationReadCommitted() throws IOException {
    ODatabaseDocumentTx db1 = new ODatabaseDocumentTx(url);
    db1.open("admin", "admin");

    ODatabaseDocumentTx db2 = new ODatabaseDocumentTx(url);
    db2.open("admin", "admin");

    ODocument record1 = new ODocument();
    record1.field("name", "This is the first version").save();

    db1.begin();
    db1.getTransaction().setIsolationLevel(OTransaction.ISOLATION_LEVEL.READ_COMMITTED);

    // RE-READ THE RECORD
    record1.getIdentity().getRecord();

    // CHANGE THE RECORD FROM DB2
    ODocument record2 = db2.load(record1.getIdentity());
    record2.field("name", "This is the second version").save();

    db1.reload(record1, null, true);

    Assert.assertEquals(record1.field("name"), "This is the second version");

    db1.close();
    db2.close();
  }
}
