package com.orientechnologies.orient.graph.gremlin;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientElement;

public class SQLGremlinTest {
	public SQLGremlinTest() {
		OGremlinHelper.global().create();
	}

	@Test
	public void f() {
		OGraphDatabase db = new OGraphDatabase("local:target/databases/gremlin");
		if (db.exists())
			db.open("admin", "admin");
		else
			db.create();

		ODocument vertex1 = (ODocument) db.createVertex().field("label", "car").save();
		ODocument vertex2 = (ODocument) db.createVertex().field("label", "pilot").save();
		ODocument edge = (ODocument) db.createEdge(vertex1, vertex2).field("label", "drives").save();

		List<?> result = db.query(new OSQLSynchQuery<Object>(
				"select gremlin('current.out.in') as value from V where out.size() > 0 limit 3"));
		System.out.println("Query result: " + result);

		result = db.query(new OSQLSynchQuery<Object>("select gremlin('current.out') as value from V"));
		System.out.println("Query result: " + result);

		result = db.query(new OSQLSynchQuery<Object>("select gremlin('current.out.in') as value from 5:1"));
		System.out.println("Query result: " + result);

		result = db.query(new OSQLSynchQuery<Object>("select gremlin('current.out(\"drives\").count()') as value from V"));
		System.out.println("Query result: " + result);

		db.close();
	}

	@Test
	public void command() {
		OGraphDatabase db = new OGraphDatabase("local:target/databases/gremlin");
		if (db.exists())
			db.open("admin", "admin");
		else
			db.create();

		List<OrientElement> result = db.command(new OCommandGremlin("g.V[0..10]")).execute();
		if (result != null) {
			for (OrientElement doc : result) {
				System.out.println(doc.getRawElement().toJSON());
			}
		}

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("par1", new Date());

		result = db.command(new OCommandSQL("select gremlin('current.outE('own').inV{ it.id == par1 }') from V")).execute(params);
		System.out.println("Command result: " + result);

		db.close();
	}
}
