package com.orientechnologies.orient.graph.gremlin;

import com.tinkerpop.blueprints.pgm.TransactionalGraph.Mode;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader;

public class TestLoadGraph {
	private static final String	INPUT_FILE	= "/graph-example-2.xml";
	private static final String	DBURL				= "local:../../databases/tinkerpop";

	// private static final String DBURL = "remote:localhost/graph";

	public static void main(String[] args) throws Exception {
		OrientGraph g = new OrientGraph(DBURL);

		System.out.println("Importing graph from file '" + INPUT_FILE + "' into database: " + g);

		g.setTransactionMode(Mode.MANUAL);

		GraphMLReader.inputGraph(g, Thread.class.getResourceAsStream(INPUT_FILE), 0, null, null, null);
		g.shutdown();
	}
}
