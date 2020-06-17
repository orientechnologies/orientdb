/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OCommandExecutorSQLCreateLinkTest {
  static ODatabaseDocumentTx db;
  private static String DB_STORAGE = "memory";
  private static String DB_NAME = "OCommandExecutorSQLCreateLinkTest";

  @BeforeClass
  public static void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
    db.close();
  }

  @Test
  public void testBasic() throws Exception {
    db.command(new OCommandSQL("create class Basic1")).execute();
    db.command(new OCommandSQL("create class Basic2")).execute();

    db.command(new OCommandSQL("insert into Basic1 set pk = 'pkb1_1', fk = 'pkb2_1'")).execute();
    db.command(new OCommandSQL("insert into Basic1 set pk = 'pkb1_2', fk = 'pkb2_2'")).execute();

    db.command(new OCommandSQL("insert into Basic2 set pk = 'pkb2_1'")).execute();
    db.command(new OCommandSQL("insert into Basic2 set pk = 'pkb2_2'")).execute();

    db.command(new OCommandSQL("CREATE LINK theLink type link FROM Basic1.fk TO Basic2.pk "))
        .execute();

    List<ODocument> result =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select pk, theLink.pk as other from Basic1 order by pk"));
    Assert.assertEquals(result.size(), 2);

    Object otherKey = result.get(0).field("other");
    Assert.assertNotNull(otherKey);

    Assert.assertEquals(otherKey, "pkb2_1");

    otherKey = result.get(1).field("other");
    Assert.assertEquals(otherKey, "pkb2_2");
  }

  @Test
  public void testInverse() throws Exception {
    db.command(new OCommandSQL("create class Inverse1")).execute();
    db.command(new OCommandSQL("create class Inverse2")).execute();

    db.command(new OCommandSQL("insert into Inverse1 set pk = 'pkb1_1', fk = 'pkb2_1'")).execute();
    db.command(new OCommandSQL("insert into Inverse1 set pk = 'pkb1_2', fk = 'pkb2_2'")).execute();
    db.command(new OCommandSQL("insert into Inverse1 set pk = 'pkb1_3', fk = 'pkb2_2'")).execute();

    db.command(new OCommandSQL("insert into Inverse2 set pk = 'pkb2_1'")).execute();
    db.command(new OCommandSQL("insert into Inverse2 set pk = 'pkb2_2'")).execute();

    db.command(
            new OCommandSQL(
                "CREATE LINK theLink TYPE LINKSET FROM Inverse1.fk TO Inverse2.pk INVERSE"))
        .execute();

    List<ODocument> result =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select pk, theLink.pk as other from Inverse2 order by pk"));
    Assert.assertEquals(result.size(), 2);

    Object otherKeys = result.get(0).field("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).get(0), "pkb1_1");

    otherKeys = result.get(1).field("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).size(), 2);
    Assert.assertTrue(((List) otherKeys).contains("pkb1_2"));
    Assert.assertTrue(((List) otherKeys).contains("pkb1_3"));
  }
}
