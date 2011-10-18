package com.orientechnologies.orient.graph.gremlin;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader;

public class TestLoadGraph {
	private static final String	INPUT_FILE	= "target/test-classes/graph-example-2.xml";
	private static final String	DBURL				= "local:target/databases/tinkerpop";
	private String							inputFile		= INPUT_FILE;
	private String							dbURL				= DBURL;

	public static void main(final String[] args) throws Exception {
		new TestLoadGraph(args).testImport();
	}

	public TestLoadGraph() {
		inputFile = INPUT_FILE;
		dbURL = DBURL;
	}

	public TestLoadGraph(final String[] args) {
		inputFile = args.length > 0 ? args[0] : INPUT_FILE;
		dbURL = args.length > 1 ? args[1] : DBURL;
	}

	@Test
	public void testImport() throws IOException, FileNotFoundException {
		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
		OGraphDatabase db = new OGraphDatabase(DBURL);
		ODatabaseHelper.deleteDatabase(db);
		OrientGraph g = new OrientGraph(dbURL);

		System.out.println("Importing graph from file '" + inputFile + "' into database: " + g + "...");

		final long startTime = System.currentTimeMillis();

		g.setMaxBufferSize(0);

		GraphMLReader.inputGraph(g, new FileInputStream(inputFile), 100000, null, null, null);

		System.out.println("Imported in " + (System.currentTimeMillis() - startTime) + "ms. Vertexes: "
				+ g.getRawGraph().countVertexes() + ", Edges: " + g.getRawGraph().countEdges());

		g.shutdown();
	}
}
