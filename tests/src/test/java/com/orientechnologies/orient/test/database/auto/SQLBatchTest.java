/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLBatchTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLBatchTest(@Optional String url) {
    super(url);
  }

  /** Issue #4349 (https://github.com/orientechnologies/orientdb/issues/4349) */
  public void createEdgeFailIfNoSourceOrTargetVertices() {
    try {
      executeBatch(
          "BEGIN\n"
              + "LET credential = INSERT INTO V SET email = '123', password = '123'\n"
              + "LET order = SELECT FROM V WHERE cannotFindThisAttribute = true\n"
              + "LET edge = CREATE EDGE E FROM $credential TO $order set crazyName = 'yes'\n"
              + "COMMIT\n"
              + "RETURN $credential");

      Assert.fail("Tx has been committed while a rollback was expected");
    } catch (OCommandExecutionException e) {

      List<OIdentifiable> result =
          database.query(new OSQLSynchQuery<Object>("select from V where email = '123'"));
      Assert.assertTrue(result.isEmpty());

      result = database.query(new OSQLSynchQuery<Object>("select from E where crazyName = 'yes'"));
      Assert.assertTrue(result.isEmpty());

    } catch (Exception e) {
      Assert.fail("Error but not what was expected");
    }
  }

  public void testInlineArray() {
    // issue #7435
    String className1 = "SQLBatchTest_testInlineArray1";
    String className2 = "SQLBatchTest_testInlineArray2";
    database.command(new OCommandSQL("CREATE CLASS " + className1 + " EXTENDS V")).execute();
    database.command(new OCommandSQL("CREATE CLASS " + className2 + " EXTENDS V")).execute();
    database
        .command(new OCommandSQL("CREATE PROPERTY " + className2 + ".foos LinkList " + className1))
        .execute();

    String script =
        ""
            + "BEGIN;"
            + "LET a = CREATE VERTEX "
            + className1
            + ";"
            + "LET b = CREATE VERTEX "
            + className1
            + ";"
            + "LET c = CREATE VERTEX "
            + className1
            + ";"
            + "CREATE VERTEX "
            + className2
            + " SET foos=[$a,$b,$c];"
            + "COMMIT";

    database.command(new OCommandScript(script)).execute();

    List<ODocument> result =
        database.query(new OSQLSynchQuery<Object>("select from " + className2));
    Assert.assertEquals(result.size(), 1);
    List foos = result.get(0).field("foos");
    Assert.assertEquals(foos.size(), 3);
    Assert.assertTrue(foos.get(0) instanceof OIdentifiable);
    Assert.assertTrue(foos.get(1) instanceof OIdentifiable);
    Assert.assertTrue(foos.get(2) instanceof OIdentifiable);
  }

  public void testInlineArray2() {
    // issue #7435
    String className1 = "SQLBatchTest_testInlineArray21";
    String className2 = "SQLBatchTest_testInlineArray22";
    database.command(new OCommandSQL("CREATE CLASS " + className1 + " EXTENDS V")).execute();
    database.command(new OCommandSQL("CREATE CLASS " + className2 + " EXTENDS V")).execute();
    database
        .command(new OCommandSQL("CREATE PROPERTY " + className2 + ".foos LinkList " + className1))
        .execute();

    String script =
        ""
            + "BEGIN;"
            + "LET a = CREATE VERTEX "
            + className1
            + ";"
            + "LET b = CREATE VERTEX "
            + className1
            + ";"
            + "LET c = CREATE VERTEX "
            + className1
            + ";"
            + "LET foos = [$a,$b,$c];"
            + "CREATE VERTEX "
            + className2
            + " SET foos= $foos;"
            + "COMMIT";

    database.command(new OCommandScript(script)).execute();

    List<ODocument> result =
        database.query(new OSQLSynchQuery<Object>("select from " + className2));
    Assert.assertEquals(result.size(), 1);
    List foos = result.get(0).field("foos");
    Assert.assertEquals(foos.size(), 3);
    Assert.assertTrue(foos.get(0) instanceof OIdentifiable);
    Assert.assertTrue(foos.get(1) instanceof OIdentifiable);
    Assert.assertTrue(foos.get(2) instanceof OIdentifiable);
  }

  private Object executeBatch(final String batch) {
    return database.command(new OCommandScript("sql", batch)).execute();
  }
}
