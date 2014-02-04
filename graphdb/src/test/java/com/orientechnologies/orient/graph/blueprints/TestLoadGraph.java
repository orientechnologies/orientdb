package com.orientechnologies.orient.graph.blueprints;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class TestLoadGraph {
  private static final String INPUT_FILE = "src/test/resources/graph-example-2.xml";
  private static final String DBURL      = "plocal:target/databases/GratefulDeadConcerts";
  private String              inputFile  = INPUT_FILE;
  private String              dbURL      = DBURL;

  public static void main(final String[] args) throws Exception {
    new TestLoadGraph(args).testImport();
  }

  public TestLoadGraph() {
    inputFile = INPUT_FILE;
    dbURL = DBURL;
  }

  private TestLoadGraph(final String[] args) {
    inputFile = args.length > 0 ? args[0] : INPUT_FILE;
    dbURL = args.length > 1 ? args[1] : DBURL;
  }

  @Test
  public void testImport() throws IOException, FileNotFoundException {
    final boolean oldKeepOpen = OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean();
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DBURL);
    ODatabaseHelper.deleteDatabase(db, "plocal");

    OrientBaseGraph g = new OrientGraphNoTx(dbURL);

    System.out.println("Importing graph from file '" + inputFile + "' into database: " + g + "...");

    final long startTime = System.currentTimeMillis();

    GraphMLReader.inputGraph(g, new FileInputStream(inputFile), 10000, null, null, null);

    System.out.println("Imported in " + (System.currentTimeMillis() - startTime) + "ms. Vertexes: " + g.countVertices());

    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(oldKeepOpen);
    //g.drop();
  }
}
