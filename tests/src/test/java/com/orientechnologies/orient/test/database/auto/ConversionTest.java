package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.graph.migration.OGraphMigration;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 3/11/14
 */
@Test
public class ConversionTest {
  public void testConversion() throws Exception {
    final ODatabaseDocumentTx database = new ODatabaseDocumentTx("local:/home/andrey/Development/orientdb/archiva.db");
    database.open("admin", "admin");

    final ODatabaseExport databaseExport = new ODatabaseExport(database, "/home/andrey/Development/orientdb/archiva.db.json",
        new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.println(iText);
          }
        });

    databaseExport.exportDatabase();
    databaseExport.close();

    final ODatabaseDocumentTx newDatabase = new ODatabaseDocumentTx("plocal:/home/andrey/Development/orientdb/new.archiva.db");
    newDatabase.create();

    final ODatabaseImport databaseImport = new ODatabaseImport(newDatabase, "/home/andrey/Development/orientdb/archiva.db.json.gz",
        new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.println(iText);
          }
        });
    databaseImport.importDatabase();

    database.close();
    databaseImport.close();

    database.open("admin", "admin");

    OGraphMigration graphMigration = new OGraphMigration(newDatabase, new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
        System.out.println(iText);
      }
    });

    graphMigration.execute();
    database.close();
  }
}