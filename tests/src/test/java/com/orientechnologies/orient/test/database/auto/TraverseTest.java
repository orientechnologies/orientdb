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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class TraverseTest {
	private OGraphDatabase	database;
	private ODocument				tomCruise;
	private ODocument				megRyan;
	private ODocument				nicoleKidman;

	@Parameters(value = "url")
	public TraverseTest(String iURL) {
		database = new OGraphDatabase(iURL);
	}

	@BeforeClass
	public void init() {
		database.open("admin", "admin");

		database.createVertexType("Movie");
		database.createVertexType("Actor");

		tomCruise = database.createVertex("Actor").field("name", "Tom Cruise");
		megRyan = database.createVertex("Actor").field("name", "Meg Ryan");
		nicoleKidman = database.createVertex("Actor").field("name", "Nicol Kidman");

		ODocument topGun = database.createVertex("Movie").field("name", "Top Gun").field("year", 1986);
		ODocument missionImpossible = database.createVertex("Movie").field("name", "Mission: Impossible").field("year", 1996);
		ODocument youHaveGotMail = database.createVertex("Movie").field("name", "You've Got Mail").field("year", 1998);

		database.createEdge(tomCruise, topGun).field("actorIn");
		database.createEdge(megRyan, topGun).field("actorIn");
		database.createEdge(tomCruise, missionImpossible).field("actorIn");
		database.createEdge(megRyan, youHaveGotMail).field("actorIn");

		database.createEdge(tomCruise, megRyan).field("friend", true);
		database.createEdge(tomCruise, nicoleKidman).field("married", true).field("year", 1990);

		tomCruise.save();
	}

	@AfterClass
	public void deinit() {
		database.close();
	}

	@Test(expectedExceptions = OCommandSQLParsingException.class)
	public void traverseOutFromActorNoWhere() {
		database.command(new OSQLSynchQuery<ODocument>("traverse out from " + tomCruise.getIdentity())).execute();
	}

	@Test
	public void traverseOutFromActor1Depth() {
		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("traverse out from " + tomCruise.getIdentity() + " where $depth <= 1")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
		}
	}

	@Test
	public void traverseDept02() {
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("traverse any() from Movie where $depth < 2"))
				.execute();

	}

	@Test
	public void traverseDept12() {
		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("traverse any() from Movie where $depth between 1 and 2")).execute();

	}
}
