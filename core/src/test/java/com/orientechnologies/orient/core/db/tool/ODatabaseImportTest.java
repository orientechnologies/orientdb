package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 23/05/16. */
public class ODatabaseImportTest {
  @Test
  public void exportImportOnlySchemaTest() throws IOException {
    final String databaseName = "test";
    final String exportDbUrl =
        "embedded:target/export_" + ODatabaseImportTest.class.getSimpleName();
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            databaseName, exportDbUrl, OCreateDatabaseUtil.TYPE_PLOCAL);

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (final ODatabaseSession db =
        orientDB.open(databaseName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
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
        "embedded:target/import_" + ODatabaseImportTest.class.getSimpleName();
    OCreateDatabaseUtil.createDatabase(databaseName, importDbUrl, OCreateDatabaseUtil.TYPE_PLOCAL);

    try (final ODatabaseSession db =
        orientDB.open(databaseName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
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

  @Test
  public void exportImportExcludeClusters() throws IOException {
    final String databaseName = "test";
    final String exportDbUrl =
        "embedded:target/export_" + ODatabaseImportTest.class.getSimpleName() + "_excludeclusters";
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            databaseName, exportDbUrl, OCreateDatabaseUtil.TYPE_PLOCAL);

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (final ODatabaseSession db =
        orientDB.open(databaseName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      db.createClass("SimpleClass");

      final ODatabaseExport export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) db,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
      export.setOptions(" -includeClusterDefinitions=false");
      export.exportDatabase();
    }

    final String importDbUrl =
        "embedded:target/import_" + ODatabaseImportTest.class.getSimpleName() + "_excludeclusters";
    OCreateDatabaseUtil.createDatabase(databaseName, importDbUrl, OCreateDatabaseUtil.TYPE_PLOCAL);

    try (final ODatabaseSession db =
        orientDB.open(databaseName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
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
}
