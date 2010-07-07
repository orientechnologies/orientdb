/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "query", sequential = true)
public class FetchPlanTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public FetchPlanTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryNoFetchPlan() {
		database.open("admin", "admin");

		final long times = OProfiler.getInstance().getStatistic("Cache.reused");
		database.command(new OSQLSynchQuery<ODocument>("select * from Profile")).execute();
		Assert.assertEquals(OProfiler.getInstance().getStatistic("Cache.reused"), times);

		database.close();
	}

	@Test
	public void queryWithFetchPlan() {
		database.open("admin", "admin");

		final long times = OProfiler.getInstance().getStatistic("Cache.reused");
		database.command(new OSQLSynchQuery<ODocument>("select * from Profile").setFetchPlan("*:-1")).execute();
		Assert.assertEquals(OProfiler.getInstance().getStatistic("Cache.reused"), times);

		database.close();
	}
}
