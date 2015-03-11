package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLoadGraph {
  private static final String INPUT_FILE = "src/test/resources/graph-example-2.xml";
  private final static Logger LOGGER = LoggerFactory.getLogger(TestLoadGraph.class);

  private String              inputFile  = INPUT_FILE;
  private String              dbURL;

  public TestLoadGraph() {
    inputFile = INPUT_FILE;
  }

  private TestLoadGraph(final String[] args) {
    inputFile = args.length > 0 ? args[0] : INPUT_FILE;
    dbURL = args.length > 1 ? args[1] : null;
  }

  @Test
  public void testImport() throws IOException, FileNotFoundException {
    String storageType = System.getProperty("storageType");

    if (storageType == null) {
      storageType = "memory";
    }

    if (dbURL == null) {
      dbURL = storageType + ":" + "target/databases/GratefulDeadConcerts";
    }


    ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbURL);
    ODatabaseHelper.deleteDatabase(db, storageType);

    OrientBaseGraph g = new OrientGraphNoTx(dbURL);

    LOGGER.debug("Importing graph from file '" + inputFile + "' into database: " + g + "...");

    final long startTime = System.currentTimeMillis();

    GraphMLReader.inputGraph(g, new FileInputStream(inputFile), 10000, null, null, null);

    LOGGER.debug("Imported in " + (System.currentTimeMillis() - startTime) + "ms. Vertexes: " + g.countVertices());

    g.command(new OCommandSQL("alter database TIMEZONE GMT")).execute();
    g.command(new OCommandSQL("alter database LOCALECOUNTRY UK")).execute();
    g.command(new OCommandSQL("alter database LOCALELANGUAGE EN")).execute();
    g.shutdown();
    // g.drop();
  }
}
