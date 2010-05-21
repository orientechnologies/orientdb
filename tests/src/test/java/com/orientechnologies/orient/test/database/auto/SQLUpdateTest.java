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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = "sql-update", sequential = true)
public class SQLUpdateTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLUpdateTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void updateWithWhereOperator() {
		database.open("admin", "admin");

		Integer records = (Integer) database.command(
				new OCommandSQL("update Profile set salary = 120.30, location = -3:2, salary_cloned = salary where surname = 'Smith'")).execute();

		Assert.assertTrue(records == 1);

		database.close();
	}

	@Test(dependsOnMethods = "updateWithWhereOperator")
	public void updateAllOperator() {
		database.open("admin", "admin");

		Long total = database.countClass("Profile");

		Integer records = (Integer) database.command(new OCommandSQL("update Profile set sex = 'male'")).execute();

		Assert.assertEquals(records.intValue(), total.intValue());

		database.close();
	}
}
