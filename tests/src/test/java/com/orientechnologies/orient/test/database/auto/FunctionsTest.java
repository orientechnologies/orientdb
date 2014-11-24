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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "function")
public class FunctionsTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public FunctionsTest(@Optional String iURL) {
    super(iURL);
  }

  @Test
  public void createFunctionBug2415() {
    OIdentifiable result = database.command(
        new OCommandSQL("create function FunctionsTest \"return a + b\" PARAMETERS [a,b] IDEMPOTENT true LANGUAGE Javascript"))
        .execute();

    final ODocument record = result.getRecord();
    record.reload();

    final List<String> parameters = record.field("parameters");

    Assert.assertNotNull(parameters);
    Assert.assertEquals(parameters.size(), 2);
  }

  @Test
  public void testFunctionDefinitionAndCall() {
    database.command(new OCommandSQL("create function testCall \"return 0;\" LANGUAGE Javascript")).execute();

    OResultSet<OIdentifiable> res1 = database.command(new OCommandSQL("select testCall()")).execute();
    Assert.assertNotNull(res1);
    Assert.assertNotNull(res1.get(0));
    Assert.assertEquals(((ODocument) res1.get(0)).field("testCall"), 0);
  }

  @Test
  public void testFunctionCacheAndReload() {
    OIdentifiable f = database.command(new OCommandSQL("create function testCache \"return 1;\" LANGUAGE Javascript")).execute();
    Assert.assertNotNull(f);

    OResultSet<OIdentifiable> res1 = database.command(new OCommandSQL("select testCache()")).execute();
    Assert.assertNotNull(res1);
    Assert.assertNotNull(res1.get(0));
    Assert.assertEquals(((ODocument) res1.get(0)).field("testCache"), 1);

    ODocument func = f.getRecord();
    func.field("code", "return 2;");
    func.save();

    OResultSet<OIdentifiable>  res2 = database.command(new OCommandSQL("select testCache()")).execute();
    Assert.assertNotNull(res2);
    Assert.assertNotNull(res2.get(0));
    Assert.assertEquals(((ODocument) res2.get(0)).field("testCache"), 2);
  }

}
