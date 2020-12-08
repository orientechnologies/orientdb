package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ODatabaseImportSimpleCompatibilityTest {
  private final String databaseName = "testBench";

  private OrientDB orientDB;

  private ODatabaseSession importDatabase;
  private ODatabaseImport importer;

  private ODatabaseExport export;

  @Test
  public void testImportExportOldEmpty() {
    final InputStream emptyDbV2 =
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("databases\\databases_2_2\\Empty.json");
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    Assert.assertEquals(0, output.size());
    this.setup(emptyDbV2, output);

    this.executeImport();
    this.executeExport();

    this.tearDown();
    Assert.assertTrue(output.size() > 0);
  }

  @Test
  public void testImportExportOldSimple() {
    final InputStream simpleDbV2 =
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("databases\\databases_2_2\\OrderCustomer-sl-0.json");
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    Assert.assertEquals(0, output.size());
    this.setup(simpleDbV2, output);

    this.executeImport();
    this.executeExport();

    Assert.assertTrue(importDatabase.getMetadata().getSchema().existsClass("OrderCustomer"));

    this.tearDown();
    Assert.assertTrue(output.size() > 0);
  }

  @Test
  public void testImportExportNewerSimple() {
    final InputStream simpleDbV2 =
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("databases\\databases_3_1\\OrderCustomer-sl-0.json");
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    Assert.assertEquals(0, output.size());
    this.setup(simpleDbV2, output);

    this.executeImport();
    this.executeExport();

    Assert.assertTrue(importDatabase.getMetadata().getSchema().existsClass("OrderCustomer"));

    this.tearDown();
    Assert.assertTrue(output.size() > 0);
  }

  private void setup(final InputStream input, final OutputStream output) {
    final String importDbUrl = "memory:target/import_" + this.getClass().getSimpleName();
    orientDB = createDatabase(databaseName, importDbUrl);

    importDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
      importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) importDatabase,
              input,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
      export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) importDatabase,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

  private void tearDown() {
    try {
      orientDB.drop(databaseName);
      orientDB.close();
    } catch (final Exception e) {
      System.out.println("Issues during teardown" + e.getMessage());
    }
  }

  private void executeImport() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
    importer.importDatabase();
  }

  public void executeExport() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
    export.setOptions(" -excludeAll -includeSchema=true");
    export.exportDatabase();
  }

  private OrientDB createDatabase(final String database, final String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(database, ODatabaseType.PLOCAL);
    return orientDB;
  }
}
