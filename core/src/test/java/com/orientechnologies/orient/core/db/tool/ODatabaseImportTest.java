package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by tglman on 23/05/16.
 */
public class ODatabaseImportTest {

  @Test
  public void exportImportOnlySchemaTest() throws IOException {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODatabaseImportTest.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("SimpleClass");
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ODatabaseExport export = new ODatabaseExport((ODatabaseDocumentInternal) db, output, new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
      }
    });

    export.setOptions(" -excludeAll -includeSchema=true");
    export.exportDatabase();
    db.drop();

    ODatabaseDocument dbImp = new ODatabaseDocumentTx("memory:import_" + ODatabaseImportTest.class.getSimpleName());
    dbImp.create();
    ODatabaseImport importer = new ODatabaseImport((ODatabaseDocumentInternal) dbImp,
        new ByteArrayInputStream(output.toByteArray()), new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {

      }
    });
    importer.importDatabase();

    assertTrue(dbImp.getMetadata().getSchema().existsClass("SimpleClass"));
    dbImp.drop();
  }

}
