package com.orientechnologies.orient.test.database.speed;

import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Scanner;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * A CLI program to test orientdb memory consumption.
 * 
 * @author Davide Cavestro
 * 
 */
public class Main {

  public static void main(final String[] args) {
    final Scanner scanner = new Scanner(System.in);
    for (;;) {
      try {
        final int rowCount = askForInt(
            "Please provide the number of rows to insert, then hit ENTER (press 'Q' or 'q' to terminate):", scanner);
        final int colCount = askForInt(
            "Please provide the number of columns to insert, then hit ENTER (press 'Q' or 'q' to terminate):", scanner);

        final int lazyUpdates = askForInt(
            "Please provide the lazyUpdates param value, then hit ENTER (press 'Q' or 'q' to terminate):", scanner);

        work(rowCount, colCount, lazyUpdates);
      } catch (final ExitRequestException e) {
        // exit
        return;
      } catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }
  }

  private static int askForInt(final String msg, final Scanner scanner) throws ExitRequestException {
    for (;;) {
      System.out.print(msg);
      try {
        return scanner.nextInt();
      } catch (final InputMismatchException e) {
        final String cmd = scanner.nextLine();
        if ("q".equalsIgnoreCase(cmd)) {
          throw new ExitRequestException();
        }
      }
    }
  }

  public static void work(final int rowCount, final int colCount, final int lazyUpdates) throws java.io.IOException {

    System.out.println("Writing " + rowCount + " rows with " + colCount + " columns and lazyUpdates set to " + lazyUpdates);

    /*
     * orientdb global configuration
     */
    OGlobalConfiguration.TX_USE_LOG.setValue(false);
    OGlobalConfiguration.TX_LOG_SYNCH.setValue(false);
    OGlobalConfiguration.TX_COMMIT_SYNCH.setValue(false);

    OGlobalConfiguration.ENVIRONMENT_CONCURRENT.setValue(false);

    OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
    // OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(500);
    OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(false);
    // OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(1000);

    OGlobalConfiguration.DB_VALIDATION.setValue(false);
    OGlobalConfiguration.FILE_MMAP_LOCK_MEMORY.setValue(false);
    OGlobalConfiguration.DB_MVCC.setValue(false);
    OGlobalConfiguration.FILE_MMAP_STRATEGY.setValue(4);

    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
    OGlobalConfiguration.INDEX_AUTO_REBUILD_AFTER_NOTSOFTCLOSE.setValue(false);

    OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.setValue(lazyUpdates);
    // OGlobalConfiguration.MVRBTREE_OPTIMIZE_THRESHOLD.setValue(1);
    // OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.setValue(5);

    /*
     * creating database
     */
    final String dbUrl = "local:/temp/database/davide";

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
    if (db.exists())
      db.open("admin", "admin").drop();

    db.create();

    try {
      final OStorage storage = db.getStorage();

      final String documentName = "fooClass";
      if (!db.getMetadata().getSchema().existsClass(documentName)) {
        /*
         * configuring db metadata
         */
        storage.command(new OCommandSQL("CREATE CLASS " + documentName));
        storage.command(new OCommandSQL("CREATE PROPERTY " + documentName + ".col0 STRING"));
        // FIXME this index retains huge amounts of data in heap
        storage.command(new OCommandSQL("CREATE INDEX " + documentName + "-col0Idx ON " + documentName + " (col0) DICTIONARY"));
      }

      /*
       * massive data insertion
       */
      db.declareIntent(new OIntentMassiveInsert());

      for (int id = 0; id < rowCount; id++) {
        final ODocument doc = new ODocument(documentName);
        for (int col = 0; col < colCount; col++) {
          String string = id
              + " with a veeeeeeeeeeeeeeeeeeeeery long string associated, just to generate stress memory consumption... ";
          StringBuilder sb = new StringBuilder();
          for (int j = 0; j < 1000; j++) {
            sb.append(string).append(" ");
          }
          doc.field("col" + col, sb.toString());
        }
        db.save(doc);
        System.out.print(".");
      }
      System.out.println("Job completed");
      db.declareIntent(null);
    } finally {
      db.close();
    }
  }

  /**
   * A system exit request.
   * 
   * @author Davide Cavestro
   * 
   */
  private static class ExitRequestException extends Exception {
  }

}