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
package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * @author Luigi Dell'Aquila
 */
@RunWith(JUnit4.class)
public class OCommandExecutorSQLDeleteVertexTest {

  private static ODatabaseDocumentTx db;

  @BeforeClass
  public static void init() throws Exception {
    db = Orient.instance().getDatabaseFactory()
        .createDatabase("graph", "memory:" + OCommandExecutorSQLDeleteVertexTest.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    final OSchema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    db.activateOnCurrentThread();
    db.drop();
    db = null;
  }

  @Before
  public void setUp() throws Exception {
    db.getMetadata().getSchema().getClass("User").truncate();
  }

  @Test
  public void testDeleteVertexLimit() throws Exception {
    // for issue #4148

    for (int i = 0; i < 10; i++) {
      db.command(new OCommandSQL("create vertex User set name = 'foo" + i + "'")).execute();
    }

    final int res = (Integer) db.command(new OCommandSQL("delete vertex User limit 4")).execute();

    List<?> result = db.query(new OSQLSynchQuery("select from User"));
    Assert.assertEquals(result.size(), 6);

  }

  @Test
  public void testDeleteVertexBatch() throws Exception {
    // for issue #4622

    for (int i = 0; i < 100; i++) {
      db.command(new OCommandSQL("create vertex User set name = 'foo" + i + "'")).execute();
    }

    final int res = (Integer) db.command(new OCommandSQL("delete vertex User batch 5")).execute();

    List<?> result = db.query(new OSQLSynchQuery("select from User"));
    Assert.assertEquals(result.size(), 0);

  }

  @Test
  public void testDeleteVertexFromSubquery() throws Exception {
    // for issue #4523

    for (int i = 0; i < 100; i++) {
      db.command(new OCommandSQL("create vertex User set name = 'foo" + i + "'")).execute();
    }

    final int res = (Integer) db.command(new OCommandSQL("delete vertex from (select from User)")).execute();

    List<?> result = db.query(new OSQLSynchQuery("select from User"));
    Assert.assertEquals(result.size(), 0);

  }

  @Test
  public void testDeleteVertexFromSubquery2() throws Exception {
    // for issue #4523

    for (int i = 0; i < 100; i++) {
      db.command(new OCommandSQL("create vertex User set name = 'foo" + i + "'")).execute();
    }

    final int res = (Integer) db.command(new OCommandSQL("delete vertex from (select from User where name = 'foo10')")).execute();

    List<?> result = db.query(new OSQLSynchQuery("select from User"));
    Assert.assertEquals(result.size(), 99);

  }


}
