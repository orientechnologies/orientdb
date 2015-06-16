/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class SQLBatchTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLBatchTest(@Optional String url) {
    super(url);
  }

  /**
   * Issue #4349 (https://github.com/orientechnologies/orientdb/issues/4349)
   */
  public void createEdgeFailIfNoSourceOrTargetVertices() {
    try {
      executeBatch("BEGIN\n" + "LET credential = INSERT INTO V SET email = '123', password = '123'\n"
          + "LET order = SELECT FROM V WHERE cannotFindThisAttribute = true\n"
          + "LET edge = CREATE EDGE E FROM $credential TO $order set crazyName = 'yes'\n" + "COMMIT\n" + "RETURN $credential");

      Assert.fail("Tx has been committed while a rollback was expected");
    } catch (OCommandExecutionException e) {

      List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>("select from V where email = '123'"));
      Assert.assertTrue(result.isEmpty());

      result = database.query(new OSQLSynchQuery<Object>("select from E where crazyName = 'yes'"));
      Assert.assertTrue(result.isEmpty());

    } catch (Exception e) {
      Assert.fail("Error but not what was expected");
    }
  }

  private Object executeBatch(final String batch) {
    return database.command(new OCommandScript("sql", batch)).execute();
  }
}
