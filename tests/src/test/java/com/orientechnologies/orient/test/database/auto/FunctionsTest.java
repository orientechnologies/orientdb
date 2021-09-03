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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "function")
public class FunctionsTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public FunctionsTest(@Optional String iURL) {
    super(iURL);
  }

  @Test
  public void createFunctionBug2415() {
    OIdentifiable result =
        database
            .command(
                new OCommandSQL(
                    "create function FunctionsTest \"return a + b\" PARAMETERS [a,b] IDEMPOTENT true LANGUAGE Javascript"))
            .execute();

    final ODocument record = result.getRecord();
    record.reload();

    final List<String> parameters = record.field("parameters");

    Assert.assertNotNull(parameters);
    Assert.assertEquals(parameters.size(), 2);
  }

  @Test
  public void testFunctionDefinitionAndCall() {
    database
        .command(new OCommandSQL("create function testCall \"return 0;\" LANGUAGE Javascript"))
        .execute();

    OLegacyResultSet<OIdentifiable> res1 =
        database.command(new OCommandSQL("select testCall()")).execute();
    Assert.assertNotNull(res1);
    Assert.assertNotNull(res1.get(0));
    Assert.assertEquals(((ODocument) res1.get(0)).<Object>field("testCall"), 0);
  }

  @Test
  public void testFunctionCacheAndReload() {
    OIdentifiable f =
        database
            .command(new OCommandSQL("create function testCache \"return 1;\" LANGUAGE Javascript"))
            .execute();
    Assert.assertNotNull(f);

    OLegacyResultSet<OIdentifiable> res1 =
        database.command(new OCommandSQL("select testCache()")).execute();
    Assert.assertNotNull(res1);
    Assert.assertNotNull(res1.get(0));
    Assert.assertEquals(((ODocument) res1.get(0)).<Object>field("testCache"), 1);

    ODocument func = f.getRecord();
    func.field("code", "return 2;");
    func.save();

    OLegacyResultSet<OIdentifiable> res2 =
        database.command(new OCommandSQL("select testCache()")).execute();
    Assert.assertNotNull(res2);
    Assert.assertNotNull(res2.get(0));
    Assert.assertEquals(((ODocument) res2.get(0)).<Object>field("testCache"), 2);
  }

  @Test
  public void testMultiThreadsFunctionCallMoreThanPool() {
    final OIdentifiable f =
        database
            .command(
                new OCommandSQL("create function testMTCall \"return 3;\" LANGUAGE Javascript"))
            .execute();
    Assert.assertNotNull(f);

    final int TOT = 1000;
    final int threadNum = OGlobalConfiguration.SCRIPT_POOL.getValueAsInteger() * 3;
    // System.out.println("Starting " + threadNum + " concurrent threads with scriptPool="
    // + OGlobalConfiguration.SCRIPT_POOL.getValueAsInteger() + " executing function for " + TOT + "
    // times");

    final long startTime = System.currentTimeMillis();

    final AtomicLong counter = new AtomicLong();

    final Thread[] threads = new Thread[threadNum];
    for (int i = 0; i < threadNum; ++i) {
      threads[i] =
          new Thread() {
            public void run() {
              for (int cycle = 0; cycle < TOT; ++cycle) {
                OLegacyResultSet<OIdentifiable> res1 =
                    database.command(new OCommandSQL("select testMTCall()")).execute();
                Assert.assertNotNull(res1);
                Assert.assertNotNull(res1.get(0));
                Assert.assertEquals(((ODocument) res1.get(0)).<Object>field("testMTCall"), 3);

                counter.incrementAndGet();
              }
            }
          };
      threads[i].start();
    }

    for (int i = 0; i < threadNum; ++i)
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    Assert.assertEquals(counter.get(), (long) threadNum * TOT);
  }

  @Test
  public void testFunctionDefinitionAndCallWithParams() {
    database
        .command(
            new OCommandSQL(
                "create function testParams \"return 'Hello ' + name + ' ' + surname + ' from ' + country;\" PARAMETERS [name,surname,country] LANGUAGE Javascript"))
        .execute();

    OLegacyResultSet<OIdentifiable> res1 =
        database.command(new OCommandSQL("select testParams('Jay', 'Miner', 'USA')")).execute();
    Assert.assertNotNull(res1);
    Assert.assertNotNull(res1.get(0));
    Assert.assertEquals(((ODocument) res1.get(0)).field("testParams"), "Hello Jay Miner from USA");

    final HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("name", "Jay");
    params.put("surname", "Miner");
    params.put("country", "USA");

    Object result =
        database
            .getMetadata()
            .getFunctionLibrary()
            .getFunction("testParams")
            .executeInContext(null, params);
    Assert.assertEquals(result, "Hello Jay Miner from USA");
  }

  @Test
  public void testMapParamToFunction() {
    database
        .command(
            new OCommandSQL(
                "create function testMapParamToFunction \"return mapParam.get('foo')[0];\" PARAMETERS [mapParam] LANGUAGE Javascript"))
        .execute();

    Map<String, Object> params = new HashMap<String, Object>();

    List theList = new ArrayList();
    theList.add("bar");
    Map theMap = new HashMap();
    theMap.put("foo", theList);
    params.put("theParam", theMap);

    OLegacyResultSet<OIdentifiable> res1 =
        database
            .command(new OCommandSQL("select testMapParamToFunction(:theParam) as a"))
            .execute(params);
    Assert.assertNotNull(res1);
    Assert.assertNotNull(res1.get(0));
    Assert.assertEquals(((ODocument) res1.get(0)).field("a"), "bar");
  }
}
