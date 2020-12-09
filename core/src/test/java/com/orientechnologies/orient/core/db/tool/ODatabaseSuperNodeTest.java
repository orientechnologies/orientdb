package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseSuperNodeTest {

  @Test
  public void exportImportOnlySchemaTest() throws IOException {
    final String databaseName = "test";
    final String exportDbUrl =
        "memory:target/export_" + ODatabaseSuperNodeTest.class.getSimpleName();
    final OrientDB orientDB = createDatabase(databaseName, exportDbUrl);

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (final ODatabaseSession db = orientDB.open(databaseName, "admin", "admin")) {
      db.createClass("SimpleClass");

      final ODatabaseExport export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) db,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
      export.setOptions(" -excludeAll -includeSchema=true");
      export.exportDatabase();
    }

    final String importDbUrl =
        "memory:target/import_" + ODatabaseSuperNodeTest.class.getSimpleName();
    createDatabase(databaseName, importDbUrl);

    try (final ODatabaseSession db = orientDB.open(databaseName, "admin", "admin")) {
      final ODatabaseImport importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) db,
              new ByteArrayInputStream(output.toByteArray()),
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
      importer.importDatabase();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("SimpleClass"));
    }
    orientDB.drop(databaseName);
    orientDB.close();
  }

  private OrientDB createDatabase(String database, String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.create(database, ODatabaseType.PLOCAL);
    return orientDB;
  }
}
