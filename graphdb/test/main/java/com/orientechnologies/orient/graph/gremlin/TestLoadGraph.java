package com.orientechnologies.orient.graph.gremlin;

import java.io.FileInputStream;

import com.tinkerpop.blueprints.pgm.TransactionalGraph.Mode;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader;

public class TestLoadGraph {
	private static final String	INPUT_FILE	= "target/classes/graph-example-2.xml";
	//private static final String	DBURL				= "local:target/databases/tinkerpop";

	private static final String DBURL = "remote:localhost/tinkerpop";

	public static void main(final String[] args) throws Exception {
		final String inputFile = args.length > 0 ? args[0] : INPUT_FILE;
		final String dbURL = args.length > 1 ? args[1] : DBURL;

		OrientGraph g = new OrientGraph(dbURL);

		System.out.println("Importing graph from file '" + inputFile + "' into database: " + g + "...");

		final long startTime = System.currentTimeMillis();

		g.setTransactionMode(Mode.MANUAL);

		GraphMLReader.inputGraph(g, new FileInputStream(inputFile), 100000, null, null, null);
		g.shutdown();

		System.out.println("Imported in " + (System.currentTimeMillis() - startTime) + "ms. Vertexes: "
				+ g.getRawGraph().countVertexes() + " Edges: " + g.getRawGraph().countEdges());

	}
}
