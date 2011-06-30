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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class SQLCreateLinkTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLCreateLinkTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void createLinktest() {
		database.open("admin", "admin");

		Assert.assertTrue((Integer) database.command(new OCommandSQL("CREATE CLASS POST")).execute() > 0);
		Assert
				.assertTrue(database.command(new OCommandSQL("INSERT INTO POST (id, title) VALUES ( 10, 'NoSQL movement' );")).execute() instanceof ODocument);
		Assert
				.assertTrue(database.command(new OCommandSQL("INSERT INTO POST (id, title) VALUES ( 20, 'New OrientDB' )")).execute() instanceof ODocument);

		Assert.assertTrue((Integer) database.command(new OCommandSQL("CREATE CLASS COMMENT")).execute() > 0);
		Assert.assertTrue(database.command(new OCommandSQL("INSERT INTO COMMENT (id, postId, text) VALUES ( 0, 10, 'First' )"))
				.execute() instanceof ODocument);
		Assert.assertTrue(database.command(new OCommandSQL("INSERT INTO COMMENT (id, postId, text) VALUES ( 1, 10, 'Second' )"))
				.execute() instanceof ODocument);
		Assert.assertTrue(database.command(new OCommandSQL("INSERT INTO COMMENT (id, postId, text) VALUES ( 21, 10, 'Another' )"))
				.execute() instanceof ODocument);
		Assert.assertTrue(database.command(new OCommandSQL("INSERT INTO COMMENT (id, postId, text) VALUES ( 41, 20, 'First again' )"))
				.execute() instanceof ODocument);
		Assert.assertTrue(database.command(new OCommandSQL("INSERT INTO COMMENT (id, postId, text) VALUES ( 82, 20, 'Second Again' )"))
				.execute() instanceof ODocument);

		Assert.assertEquals(((Number) database.command(new OCommandSQL("CREATE LINK comments FROM comment.postId To post.id INVERSE"))
				.execute()).intValue(), 5);

		Assert.assertEquals(((Number) database.command(new OCommandSQL("UPDATE comment REMOVE postId")).execute()).intValue(), 5);

		database.close();
	}
}
