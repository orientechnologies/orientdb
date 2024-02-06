/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCommandExecutorSQLDeleteVertexTest extends BaseMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();
    final OSchema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
  }

  @Test
  public void testDeleteVertexLimit() throws Exception {
    // for issue #4148

    for (int i = 0; i < 10; i++) {
      db.command("create vertex User set name = 'foo" + i + "'").close();
    }

    db.command("delete vertex User limit 4").close();

    OResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 6);
  }

  @Test
  public void testDeleteVertexBatch() throws Exception {
    // for issue #4622

    for (int i = 0; i < 100; i++) {
      db.command("create vertex User set name = 'foo" + i + "'").close();
    }

    final int res = (Integer) db.command(new OCommandSQL("delete vertex User batch 5")).execute();

    List<?> result = db.query(new OSQLSynchQuery("select from User"));
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testDeleteVertexWithEdgeRid() throws Exception {
    List<ODocument> edges = db.command(new OCommandSQL("select from e limit 1")).execute();
    try {
      final int res =
          (Integer)
              db.command(new OCommandSQL("delete vertex [" + edges.get(0).getIdentity() + "]"))
                  .execute();
      Assert.fail("Error on deleting a vertex with a rid of an edge");
    } catch (Exception e) {
      // OK
    }
  }

  @Test
  public void testDeleteVertexFromSubquery() throws Exception {
    // for issue #4523

    for (int i = 0; i < 100; i++) {
      db.command("create vertex User set name = 'foo" + i + "'").close();
    }

    db.command("delete vertex from (select from User)").close();
    OResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testDeleteVertexFromSubquery2() throws Exception {
    // for issue #4523

    for (int i = 0; i < 100; i++) {
      db.command("create vertex User set name = 'foo" + i + "'").close();
    }

    db.command("delete vertex from (select from User where name = 'foo10')").close();

    OResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 99);
  }
}
