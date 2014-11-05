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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "query", sequential = true)
public class FetchPlanTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public FetchPlanTest(@Optional String url) {
    super(url);
  }

  @Test
  public void queryNoFetchPlan() {
		createBasicTestSchema();

    final long times = Orient.instance().getProfiler().getCounter("Cache.reused");

    database.getLocalCache().clear();
    List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>("select * from Profile"));
    Assert.assertEquals(Orient.instance().getProfiler().getCounter("Cache.reused"), times);

    ORID linked;
    for (ODocument d : resultset) {
      linked = ((ORID) d.field("location", ORID.class));
      if (linked != null)
        Assert.assertNull(database.getLocalCache().findRecord(linked));
    }
  }

  @Test(dependsOnMethods = "queryNoFetchPlan")
  public void queryWithFetchPlan() {
    final long times = Orient.instance().getProfiler().getCounter("Cache.reused");
    List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>("select * from Profile").setFetchPlan("*:-1"));
    Assert.assertEquals(Orient.instance().getProfiler().getCounter("Cache.reused"), times);

    ODocument linked;
    for (ODocument d : resultset) {
      linked = ((ODocument) d.field("location"));
      if (linked != null)
        Assert.assertNotNull(database.getLocalCache().findRecord(linked.getIdentity()));
    }
  }
}
