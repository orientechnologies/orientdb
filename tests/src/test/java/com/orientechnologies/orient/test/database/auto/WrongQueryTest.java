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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "query", sequential = true)
public class WrongQueryTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public WrongQueryTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryOpen() {
		database.open("admin", "admin");
	}

	@Test(dependsOnMethods = "queryOpen", expectedExceptions = OQueryParsingException.class)
	public void queryFieldOperatorNotSupported() {
		database.command(new OSQLSynchQuery<ODocument>("select * from Account where name.not() like 'G%'")).execute();
	}

	@Test(dependsOnMethods = "queryFieldOperatorNotSupported")
	public void queryEnd() {
		database.close();
	}
}
