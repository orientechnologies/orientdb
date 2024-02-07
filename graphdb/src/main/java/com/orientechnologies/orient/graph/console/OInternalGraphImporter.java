package com.orientechnologies.orient.graph.console;

import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import java.io.FileNotFoundException;
import java.io.IOException;

public class OInternalGraphImporter {

  public static void main(final String[] args) throws Exception {
    String inputFile = args.length > 0 ? args[0] : null;
    String dbURL = args.length > 1 ? args[1] : null;

    new OInternalGraphImporter().runImport(inputFile, dbURL);
    Orient.instance().shutdown();
  }

  public void runImport(String inputFile, String dbURL) throws IOException, FileNotFoundException {

    if (inputFile == null) throw new OSystemException("needed an input file as first argument");

    if (dbURL == null) throw new OSystemException("needed an database location as second argument");

    ODatabaseDocumentInternal db = new ODatabaseDocumentTx(dbURL);
    ODatabaseHelper.deleteDatabase(db, db.getType());

    OrientBaseGraph g = OrientGraphFactory.getNoTxGraphImplFactory().getGraph(dbURL);

    System.out.println("Importing graph from file '" + inputFile + "' into database: " + g + "...");

    final long startTime = System.currentTimeMillis();

    OConsoleDatabaseApp console =
        new OGremlinConsole(new String[] {"import database " + inputFile})
            .setCurrentDatabase((ODatabaseDocumentInternal) g.getRawGraph());
    console.run();

    System.out.println(
        "Imported in "
            + (System.currentTimeMillis() - startTime)
            + "ms. Vertexes: "
            + g.countVertices());

    g.sqlCommand("alter database TIMEZONE 'GMT'").close();
    g.sqlCommand("alter database LOCALECOUNTRY 'UK'").close();
    g.sqlCommand("alter database LOCALELANGUAGE 'EN'").close();
    g.shutdown();
  }
}
