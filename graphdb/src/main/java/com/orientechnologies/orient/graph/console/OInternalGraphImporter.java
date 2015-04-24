package com.orientechnologies.orient.graph.console;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class OInternalGraphImporter {

  public static void main(final String[] args) throws Exception {
    String inputFile = args.length > 0 ? args[0] : null;
    String dbURL = args.length > 1 ? args[1] : null;

    new OInternalGraphImporter().runImport(inputFile, dbURL);
  }

  public void runImport(String inputFile, String dbURL) throws IOException, FileNotFoundException {

    if (inputFile == null)
      throw new OException("needed an input file as first argument");

    if (dbURL == null)
      throw new OException("needed an database location as second argument");

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbURL);
    ODatabaseHelper.deleteDatabase(db, db.getStorage().getType());

    OrientBaseGraph g = new OrientGraphNoTx(dbURL);

    System.out.println("Importing graph from file '" + inputFile + "' into database: " + g + "...");

    final long startTime = System.currentTimeMillis();

    GraphMLReader.inputGraph(g, new FileInputStream(inputFile), 10000, null, null, null);

    System.out.println("Imported in " + (System.currentTimeMillis() - startTime) + "ms. Vertexes: " + g.countVertices());

    g.command(new OCommandSQL("alter database TIMEZONE GMT")).execute();
    g.command(new OCommandSQL("alter database LOCALECOUNTRY UK")).execute();
    g.command(new OCommandSQL("alter database LOCALELANGUAGE EN")).execute();
    g.shutdown();
  }
}
