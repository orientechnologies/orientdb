package com.orientechnologies.orient.test.database.speed;

import java.util.Set;

import junit.framework.Assert;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class GraphDatabaseSuperNodeSpeedTest {

	private static final String	DEFAULT_DB_URL			= "local:database/graphtest";
	private static final String	DEFAULT_DB_USER			= "admin";
	private static final String	DEFAULT_DB_PASSWORD	= "admin";
	private static final int		MAX									= 1000000;
	private OGraphDatabase			database;

	@BeforeClass
	public void setUpClass() {
		database = new OGraphDatabase(DEFAULT_DB_URL);
		if (database.exists())
			database.delete();

		database.create();
		database.close();

		database.open(DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
	}

	@AfterClass
	public void tearDownClass() {
		database.close();
	}

	@Test
	public void saveEdges() {
		database.declareIntent(new OIntentMassiveInsert());

		ODocument v = database.createVertex();
		v.field("name", "superNode");

		long insertBegin = System.currentTimeMillis();

		long begin = insertBegin;
		for (int i = 1; i <= MAX; ++i) {
			database.createEdge(v, database.createVertex().field("id", i)).save();
			if (i % 100000 == 0) {
				final long now = System.currentTimeMillis();
				System.out.printf("\nInserted %d edges, elapsed %d ms. v.out=%d", i, now - begin, ((Set<?>) v.field("out")).size());
				begin = System.currentTimeMillis();
			}
		}
		database.declareIntent(null);

		long now = System.currentTimeMillis();
		System.out.printf("\nInsertion completed in %dms. DB edges %d, DB vertices %d", now - begin, database.countEdges(),
				database.countVertexes());

		int i = 1;
		for (OIdentifiable e : database.getOutEdges(v)) {
			Assert.assertEquals(database.getInVertex(e).field("id"), i);
			if (i % 100000 == 0) {
				now = System.currentTimeMillis();
				System.out.printf("\nRead %d edges and %d vertices, elapsed %d ms", i, i, now - begin);
				begin = System.currentTimeMillis();
			}
			i++;
		}

	}
}