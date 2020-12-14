package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseSuperNodeTest {

  public static void main(String[] args) {
    final ODatabaseSuperNodeTest tester = new ODatabaseSuperNodeTest();

    final String databaseName = "superNode_export";
    final String exportDbUrl =
        "memory:target/export_" + ODatabaseSuperNodeTest.class.getSimpleName();
    final OrientDB orientDB = tester.createDatabase(databaseName, exportDbUrl);

    final List<Integer> numberEdges = Arrays.asList(100000, 500000, 1000000, 5000000, 10000000);
    final List<Long> results = new ArrayList<>(numberEdges.size());

    for (int numberEdge : numberEdges) {
      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      try (final ODatabaseSession session = orientDB.open(databaseName, "admin", "admin")) {
        session.createClassIfNotExist("SuperNodeClass", "V");
        session.createClassIfNotExist("NonSuperEdgeClass", "E");

        // session.begin();
        final OVertex fromNode = session.newVertex("SuperNodeClass");
        fromNode.save();
        final OVertex toNode = session.newVertex("SuperNodeClass");
        toNode.save();

        for (int i = 0; i < numberEdge; i++) {
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
        System.out.println("Export-" + numberEdge + "(ms)=" + time);

        Thread.sleep(2000);
      } catch (final IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < results.size(); i++) {
      System.out.println("Export-" + numberEdges.get(i) + "(ms)=" + results.get(i));
    }

    /*final String importDbUrl =
        "memory:target/import_" + ODatabaseSuperNodeTest.class.getSimpleName();
    tester.createDatabase(databaseName, importDbUrl);

    try (final ODatabaseSession db = orientDB.open(databaseName, "admin", "admin")) {
      final ODatabaseImport importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) db,
              "export_ODatabaseSuperNodeTest/" + databaseName,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
      importer.importDatabase();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("SuperNodeClass"));
    } catch (final IOException e) {
      e.printStackTrace();
    }*/
    orientDB.drop(databaseName);
    // orientDB.drop(importDatabaseName);
    orientDB.close();
  }

  private OrientDB createDatabase(final String database, final String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.create(database, ODatabaseType.PLOCAL);
    return orientDB;
  }
}
