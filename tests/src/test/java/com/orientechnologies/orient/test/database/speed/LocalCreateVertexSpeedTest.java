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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import java.util.Date;
import org.testng.annotations.Test;

@Test
public class LocalCreateVertexSpeedTest extends OrientMonoThreadTest {
  private OrientBaseGraph database;
  private Date date = new Date();

  public LocalCreateVertexSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Test(enabled = false)
  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalCreateVertexSpeedTest test = new LocalCreateVertexSpeedTest();
    test.data.go(test);
  }

  @Override
  @Test(enabled = false)
  public void init() {
    final OrientGraphFactory factory = new OrientGraphFactory(System.getProperty("url"));
    factory.setStandardElementConstraints(false);

    if (factory.exists()) factory.drop();

    database = factory.getNoTx();

    database.createVertexType("Account");

    database.getRawGraph().declareIntent(new OIntentMassiveInsert());
  }

  @Override
  @Test(enabled = false)
  public void cycle() {
    database.addVertex(
        "class:Account",
        "id",
        data.getCyclesDone(),
        "name",
        "Luca",
        "surname",
        "Garulli",
        "birthDate",
        date,
        "salary",
        3000f + data.getCyclesDone());

    if (data.getCyclesDone() == data.getCycles() - 1) database.commit();
  }

  @Override
  @Test(enabled = false)
  public void deinit() {
    System.out.println(Orient.instance().getProfiler().dump());

    if (database != null) database.shutdown();
    super.deinit();
  }
}
