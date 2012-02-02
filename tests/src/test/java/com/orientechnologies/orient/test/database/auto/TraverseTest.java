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
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
@SuppressWarnings("unused")
public class TraverseTest {
	private static final int	TOTAL_ELEMENTS	= 12;
	private OGraphDatabase		database;
	private ODocument					tomCruise;
	private ODocument					megRyan;
	private ODocument					nicoleKidman;

	@Parameters(value = "url")
	public TraverseTest(@Optional(value = "memory:test") String iURL) {
		database = new OGraphDatabase(iURL);
	}

	@BeforeClass
	public void init() {
		if ("memory:test".equals(database.getURL()))
			database.create();
		else
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

	public void traverseAllFromActorNoWhere() {
		List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse * from " + tomCruise.getIdentity()))
				.execute();
		Assert.assertEquals(result1.size(), TOTAL_ELEMENTS);
	}

	public void traverseOutFromOneActorNoWhere() {
		database.command(new OSQLSynchQuery<ODocument>("traverse out from " + tomCruise.getIdentity())).execute();
	}

	@Test
	public void traverseOutFromActor1Depth() {
		List<ODocument> result1 = database.command(
				new OSQLSynchQuery<ODocument>("traverse out from " + tomCruise.getIdentity() + " where $depth <= 1")).execute();

		Assert.assertTrue(result1.size() != 0);

		for (ODocument d : result1) {
		}
	}

	@Test
	public void traverseDept02() {
		List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse any() from Movie where $depth < 2"))
				.execute();

	}

	@Test
	public void traverseMoviesOnly() {
		List<ODocument> result1 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie ) where @class = 'Movie'")).execute();
		Assert.assertTrue(result1.size() > 0);
		for (ODocument d : result1) {
			Assert.assertEquals(d.getClassName(), "Movie");
		}
	}

	@Test
	public void traversePerClassFields() {
		List<ODocument> result1 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse V.out, E.in from " + tomCruise.getIdentity() + ") where @class = 'Movie'"))
				.execute();
		Assert.assertTrue(result1.size() > 0);
		for (ODocument d : result1) {
			Assert.assertEquals(d.getClassName(), "Movie");
		}
	}

	@Test
	public void traverseMoviesOnlyDepth() {
		List<ODocument> result1 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse * from " + tomCruise.getIdentity()
						+ " where $depth <= 1 ) where @class = 'Movie'")).execute();
		Assert.assertTrue(result1.isEmpty());

		List<ODocument> result2 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse * from " + tomCruise.getIdentity()
						+ " where $depth <= 2 ) where @class = 'Movie'")).execute();
		Assert.assertTrue(result2.size() > 0);
		for (ODocument d : result2) {
			Assert.assertEquals(d.getClassName(), "Movie");
		}

		List<ODocument> result3 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse * from " + tomCruise.getIdentity() + " ) where @class = 'Movie'"))
				.execute();
		Assert.assertTrue(result3.size() > 0);
		Assert.assertTrue(result3.size() > result2.size());
		for (ODocument d : result3) {
			Assert.assertEquals(d.getClassName(), "Movie");
		}
	}

	@Test
	public void traverseSelect() {
		List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse * from ( select from Movie )")).execute();
		Assert.assertEquals(result1.size(), TOTAL_ELEMENTS);
	}

	@Test
	public void traverseSelectAndTraverseNested() {
		List<ODocument> result1 = database.command(
				new OSQLSynchQuery<ODocument>("traverse * from ( select from ( traverse * from " + tomCruise.getIdentity()
						+ " where $depth <= 2 ) where @class = 'Movie' )")).execute();
		Assert.assertEquals(result1.size(), TOTAL_ELEMENTS);
	}

	@Test
	public void traverseIterating() {
		int cycles = 0;
		for (OIdentifiable id : new OSQLSynchQuery<ODocument>("traverse * from Movie where $depth < 2")) {
			cycles++;
		}
		Assert.assertTrue(cycles > 0);
	}

	@Test
	public void traverseSelectIterable() {
		int cycles = 0;
		for (OIdentifiable id : new OSQLSynchQuery<ODocument>("select from ( traverse * from Movie where $depth < 2 )")) {
			cycles++;
		}
		Assert.assertTrue(cycles > 0);
	}

	@Test
	public void traverseSelectNoInfluence() {
		List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse any() from Movie where $depth < 2"))
				.execute();
		List<ODocument> result2 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie where $depth < 2 )")).execute();
		List<ODocument> result3 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie where $depth < 2 ) where true")).execute();
		List<ODocument> result4 = database.command(
				new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie where $depth < 2 and ( true = true ) ) where true"))
				.execute();

		Assert.assertEquals(result1, result2);
		Assert.assertEquals(result1, result3);
		Assert.assertEquals(result1, result4);
	}

}
