package com.orientechnologies.orient.graph.gremlin;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;

public class RemoteGremlinTest {
	private final OServer		server;
	private OGraphDatabase	graphDatabase;

	public RemoteGremlinTest() throws Exception {
		OGremlinHelper.global().create();
		server = OServerMain.create();
	}

	@BeforeClass
	public void setUp() throws Exception {
		server.startup(new File(getClass().getResource("db-config.xml").getFile()));
    server.activate();
	}

	@AfterClass
	public void tearDown() throws Exception {
		if (server != null)
			server.shutdown();
	}

	@BeforeMethod
	public void beforeMethod() {
		graphDatabase = new OGraphDatabase("remote:localhost/tinkerpop");
		graphDatabase.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() {
		graphDatabase.close();
	}

	@Test
	public void function() throws IOException {
		ODocument vertex1 = (ODocument) graphDatabase.createVertex().field("label", "car").save();
		ODocument vertex2 = (ODocument) graphDatabase.createVertex().field("label", "pilot").save();
		ODocument edge = (ODocument) graphDatabase.createEdge(vertex1, vertex2).field("label", "drives").save();

		List<?> result = graphDatabase.query(new OSQLSynchQuery<Object>(
				"select gremlin('current.out.in') as value from V where out.size() > 0 limit 3"));
		System.out.println("Query result: " + result);

		result = graphDatabase.query(new OSQLSynchQuery<Object>("select gremlin('current.out') as value from V"));
		System.out.println("Query result: " + result);

		int clusterId = graphDatabase.getVertexBaseClass().getDefaultClusterId();

		result = graphDatabase.query(new OSQLSynchQuery<Object>("select gremlin('current.out.in') as value from " + clusterId + ":1"));
		System.out.println("Query result: " + result);

		result = graphDatabase.query(new OSQLSynchQuery<Object>("select gremlin('current.out(\"drives\").count()') as value from V"));
		System.out.println("Query result: " + result);
	}

	@Test
	public void command() throws IOException {
		List<OIdentifiable> result = graphDatabase.command(new OCommandGremlin("g.V[0..10]")).execute();
		if (result != null) {
			for (OIdentifiable doc : result) {
				System.out.println(doc.getRecord().toJSON());
			}
		}

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("par1", 100);

		result = graphDatabase.command(new OCommandSQL("select gremlin('current.out.filter{ it.performances > par1 }') from V")).execute(
				params);
		System.out.println("Command result: " + result);
	}

}
