package com.orientechnologies.orient.graph.gremlin;

import java.io.IOException;
import java.util.List;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class LocalGraphDbTest {
	public LocalGraphDbTest() {
		OGremlinHelper.global().create();
	}

	@Test
	public void multipleDatabasesSameThread() throws IOException {
		String db1URL = "local:target/databases/tinkerpop";
		String db2URL = "local:target/databases/tinkerpop";// + System.currentTimeMillis();

		OGraphDatabase db1 = OGraphDatabasePool.global().acquire(db1URL, "admin", "admin");
		ODocument doc1 = db1.createVertex();

		doc1.field("key", "value");
		doc1.save();
		db1.close();

		OGraphDatabase db2 = OGraphDatabasePool.global().acquire(db2URL, "admin", "admin");

		ODocument doc2 = db2.createVertex();
		doc2.field("key", "value");
		doc2.save();
		db2.close();

		db1 = OGraphDatabasePool.global().acquire(db1URL, "admin", "admin"); // this line throws the Exception

		final List<?> result = db1.query( new OSQLSynchQuery<ODocument>("select out[weight=3].size() from V where out.size() > 0"));
		
		doc1 = db1.createVertex();
		doc1.field("newkey", "newvalue");
		doc1.save();
		db1.close();
	}
}
