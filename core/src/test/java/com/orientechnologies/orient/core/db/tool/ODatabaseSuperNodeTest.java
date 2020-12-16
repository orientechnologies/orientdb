package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;

public class ODatabaseSuperNodeTest {

  public static void main(String[] args) {
    final ODatabaseSuperNodeTest tester = new ODatabaseSuperNodeTest();

    final List<Integer> numberEdges =
        Arrays.asList(1); // 100000, 500000, 1000000, 5000000, 10000000
    final List<Long> results = new ArrayList<>(numberEdges.size());

    for (int numberEdge : numberEdges) {
      final String databaseName = "superNode_export";
      final String exportDbUrl =
          "memory:target/export_" + ODatabaseSuperNodeTest.class.getSimpleName();
      OrientDB orientDB = tester.createDatabase(databaseName, exportDbUrl);

      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      try {
        testExportDatabase(numberEdge, results, databaseName, orientDB, output);
        for (int i = 0; i < results.size(); i++) {
          System.out.println("Export-" + numberEdge + "(ms)=" + results.get(i));
        }
      } finally {
        orientDB.drop(databaseName);
        orientDB.close();
      }

      final String importDbUrl =
          "memory:target/import_" + ODatabaseSuperNodeTest.class.getSimpleName();
      orientDB = tester.createDatabase(databaseName + "_reImport", importDbUrl);

      try (final ODatabaseSession db =
          orientDB.open(databaseName + "_reImport", "admin", "admin")) {
        final ODatabaseImport importer =
            new ODatabaseImport(
                (ODatabaseDocumentInternal) db,
                new ByteArrayInputStream(output.toByteArray()),
                new OCommandOutputListener() {
                  @Override
                  public void onMessage(String iText) {}
                });
        final long start = System.nanoTime();
        importer.importDatabase();
        final long time = (System.nanoTime() - start) / 1000000;
        System.out.println("Import-" + "(ms)=" + time); // + numberEdge +
        Assert.assertTrue(db.getMetadata().getSchema().existsClass("SuperNodeClass"));
      } catch (final IOException e) {
        e.printStackTrace();
      } finally {
        orientDB.drop(databaseName + "_reImport");
        orientDB.close();
      }
    }
  }

  private static void testExportDatabase(
      final int edgeNumber,
      final List<Long> results,
      final String databaseName,
      final OrientDB orientDB,
      final OutputStream output) {

    try (final ODatabaseSession session = orientDB.open(databaseName, "admin", "admin")) {
      session.createClassIfNotExist("SuperNodeClass", "V");
      session.createClassIfNotExist("NonSuperEdgeClass", "E");

      // session.begin();
      final OVertex fromNode = session.newVertex("SuperNodeClass");
      fromNode.save();
      final OVertex toNode = session.newVertex("SuperNodeClass");
      toNode.save();

      for (int i = 0; i < edgeNumber; i++) {
        final OEdge edge = session.newEdge(fromNode, toNode, "NonSuperEdgeClass");
        edge.save();
      }
      session.commit();

      final ODatabaseExport export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) session,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
      // export.setOptions(" -excludeAll -includeSchema=true");
      // List of export options can be found in `ODatabaseImpExpAbstract`
      export.setOptions(" -includeSchema=true");
      final long start = System.nanoTime();
      export.exportDatabase();
      final long time = (System.nanoTime() - start) / 1000000;
      results.add(time);
      System.out.println("Export-" + edgeNumber + "(ms)=" + time);

      Thread.sleep(2000);
    } catch (final IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private OrientDB createDatabase(final String database, final String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.create(database, ODatabaseType.PLOCAL);
    return orientDB;
  }
}
