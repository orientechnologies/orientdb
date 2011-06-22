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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-delete")
public class SQLDeleteTest {
	private ODatabaseDocument	database;
	private String						url;

	@Parameters(value = "url")
	public SQLDeleteTest(String iURL) {
		url = iURL;
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void deleteWithWhereOperator() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("insert into Profile (sex, salary) values ('female', 2100)")).execute();

		final Long total = database.countClass("Profile");

		final List<ODocument> resultset = database.query(new OSQLSynchQuery<Object>(
				"select from Profile where sex = 'female' and salary = 2100"));

		final Number records = (Number) database.command(new OCommandSQL("delete from Profile where sex = 'female' and salary = 2100"))
				.execute();

		Assert.assertEquals(records.intValue(), resultset.size());

		Assert.assertEquals(database.countClass("Profile"), total - records.intValue());

		database.close();
	}

	@Test
	public void deleteInPool() {
		ODatabaseDocumentTx db = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final Long total = db.countClass("Profile");

		final List<ODocument> resultset = db.query(new OSQLSynchQuery<Object>(
				"select from Profile where sex = 'male' and salary > 120 and salary <= 133"));

		final Number records = (Number) db.command(
				new OCommandSQL("delete from Profile where sex = 'male' and salary > 120 and salary <= 133")).execute();

		Assert.assertEquals(records.intValue(), resultset.size());

		Assert.assertEquals(db.countClass("Profile"), total - records.intValue());

		db.close();
	}
}
