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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = "sql-delete", sequential = true)
public class SQLCommandsTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLCommandsTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void createProperty() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("create property account.timesheet string")).execute();

		Assert.assertEquals(database.getMetadata().getSchema().getClass("account").getProperty("timesheet").getType(), OType.STRING);

		database.close();
	}

	@Test(dependsOnMethods = "createProperty")
	public void createLinkedClassProperty() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("create property account.knows embeddedmap account")).execute();

		Assert.assertEquals(database.getMetadata().getSchema().getClass("account").getProperty("knows").getType(), OType.EMBEDDEDMAP);
		Assert.assertEquals(database.getMetadata().getSchema().getClass("account").getProperty("knows").getLinkedClass(), database
				.getMetadata().getSchema().getClass("account"));

		database.close();
	}

	@Test(dependsOnMethods = "createLinkedClassProperty")
	public void createLinkedTypeProperty() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("create property account.tags embeddedlist string")).execute();

		Assert.assertEquals(database.getMetadata().getSchema().getClass("account").getProperty("tags").getType(), OType.EMBEDDEDLIST);
		Assert.assertEquals(database.getMetadata().getSchema().getClass("account").getProperty("tags").getLinkedType(), OType.STRING);

		database.close();
	}

	@Test(dependsOnMethods = "createLinkedTypeProperty")
	public void removeProperty() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("drop property account.timesheet")).execute();
		database.command(new OCommandSQL("drop property account.tags")).execute();

		Assert.assertFalse(database.getMetadata().getSchema().getClass("account").existsProperty("timesheet"));
		Assert.assertFalse(database.getMetadata().getSchema().getClass("account").existsProperty("tags"));

		database.close();
	}
}
