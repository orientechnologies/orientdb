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

import java.util.Collection;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = "sql-findReferences")
public class SQLFindReferencesTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLFindReferencesTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void findSimpleReference() {
		if (database.isClosed())
			database.open("admin", "admin");

		Collection<ORID> result = database.command(new OCommandSQL("find references 14:58")).execute();

		Assert.assertTrue(result.size() == 1);
		Assert.assertTrue(result.iterator().next().toString().equals("#13:54"));

		result = database.command(new OCommandSQL("find references 19:0")).execute();

		Assert.assertTrue(result.size() == 2);
		ORID rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals("#22:0") || rid.toString().equals("#21:0"));
		rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals("#22:0") || rid.toString().equals("#21:0"));

		result = database.command(new OCommandSQL("find references 9:0")).execute();
		Assert.assertTrue(result.size() == 0);

		result.clear();
		result = null;

		database.close();
	}

	@Test
	public void findReferenceByClassAndClusters() {
		if (database.isClosed())
			database.open("admin", "admin");

		Collection<ORID> result = database.command(new OCommandSQL("find references #19:0 [GraphCar]")).execute();

		Assert.assertEquals(result.size(), 1);
		Assert.assertTrue(result.iterator().next().toString().equals("#21:0"));

		result = database.command(new OCommandSQL("find references 19:0 [company,cluster:GraphMotocycle]")).execute();

		Assert.assertTrue(result.size() == 1);
		Assert.assertTrue(result.iterator().next().toString().equals("#22:0"));

		result = database.command(new OCommandSQL("find references 19:0 [company,account,cluster:OGraphEdge]")).execute();

		Assert.assertTrue(result.size() == 0);

		result.clear();
		result = null;

		database.close();
	}

}
