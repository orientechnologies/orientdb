package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author PatrickJMcCarty
 * @since 16.05.13
 */
public class GiantFileTest {
  private static final boolean RECREATE_DATABASE = true;
  private static final String DATABASE_NAME = "GiantFileTest";
  private static ODatabaseDocumentTx db = null;

  public static void main(final String[] args) throws Exception {
    OGlobalConfiguration.DISK_CACHE_SIZE.setValue(1024);
    try {
      db = new ODatabaseDocumentTx("plocal:" + DATABASE_NAME);
      if (db.exists() && RECREATE_DATABASE) {
        db.open("admin", "admin");
        db.drop();
        System.out.println("Dropped database.");
      }
      if (!db.exists()) {
        db.create();
        System.out.println("Created database.");

        final OSchema schema = db.getMetadata().getSchema();
        // Create class for storing files.
        final OClass fileClass = schema.createClass("File");
        fileClass
            .createProperty("FileName", OType.STRING)
            .setMandatory(true)
            .setNotNull(true)
            .setMin("1");
        fileClass
            .createProperty("FileSize", OType.LONG)
            .setMandatory(true)
            .setNotNull(true)
            .setMin("0");
        // ORecordBytes is a special low-level class defined by OrientDB for efficient storage of
        // raw data.
        fileClass
            .createProperty("DataChunks", OType.LINKLIST, OType.getTypeByClass(ORecordBytes.class))
            .setMandatory(true)
            .setNotNull(false);
        System.out.println("Created schema.");
      } else {
        db.open("admin", "admin");
      }

      final File giantFile = new File("giantFile.bin");

      // Create the giant file if it doesn't already exist.
      if (!giantFile.exists()) {
        // 2 GiB
        final long fileSize = (long) 2 * 1024 * 1024 * 1024;
        final long createFileStartTime = System.currentTimeMillis();
        createGiantFile(giantFile, fileSize);
        final long createFileMs = System.currentTimeMillis() - createFileStartTime;
        System.out.printf(
            "Finished creating giant file in %f seconds.\n", (float) createFileMs / 1000);
      }

      // Save the metadata about the file.
      final ODocument fileDoc = db.newInstance("File");
      fileDoc.field("FileName", giantFile.getName());
      fileDoc.field("FileSize", giantFile.length());
      fileDoc.field("DataChunks", (byte[]) null);
      fileDoc.save();

      // Store the actual file data.
      final long storeFileStartTime = System.currentTimeMillis();
      storeFileData(fileDoc, giantFile);
      final long storeFileMs = System.currentTimeMillis() - storeFileStartTime;

      System.out.printf("Finished storing giant file in %f seconds.\n", (float) storeFileMs / 1000);
    } finally {
      db.close();
    }
  }

  private static void createGiantFile(final File file, final long fileSize) throws Exception {
    final int PAGE_SIZE = 4096;
    final Random rng = new Random();
    byte currentPercent = 0;
    final RandomAccessFile raf = new RandomAccessFile(file, "rw");

    try {
      final int fullPages = (int) (fileSize / PAGE_SIZE);
      final int totalPages;
      if (fileSize > (fullPages * PAGE_SIZE)) {
        totalPages = fullPages + 1;
      } else {
        totalPages = fullPages;
      }

      // tell the OS how big the file will be (may or may not have any effect?)
      raf.setLength(fileSize);

      final byte[] buffer = new byte[PAGE_SIZE];

      // write the full pages
      for (int i = 1; i <= fullPages; i++) {
        // Fill buffer with random data
        rng.nextBytes(buffer);

        raf.write(buffer, 0, PAGE_SIZE);
        final byte percent = (byte) (i * 100 / totalPages);
        // only report progress if it has changed
        if (percent > currentPercent) {
          System.out.printf("Create Giant File: %d%%\n", percent);
          currentPercent = percent;
        }
      }

      // Fill buffer with random data
      rng.nextBytes(buffer);

      // write the final partial page (if any)
      raf.write(buffer, 0, (int) (fileSize - (fullPages * PAGE_SIZE)));

      // report 100% progress if we haven't already
      if (currentPercent < 100) {
        System.out.printf("Create Giant File: 100%%\n");
      }
    } catch (final IOException ex) {
      throw new Exception("Failed to create giant file", ex);
    } finally {
      raf.close();
    }
  }

  private static void storeFileData(final ODocument fileDoc, final File file) throws Exception {
    // To avoid overwriting a stored file, DataChunks must be null.
    final List<ORID> existingChunks = fileDoc.field("DataChunks");
    if (existingChunks != null) {
      final String fileName = fileDoc.field("FileName");
      throw new RuntimeException(
          "File record already has data; overwrite not allowed! fileName: " + fileName);
    }

    // TODO: is this assumption ok?
    // Get the currently open database for this thread and set intent.
    final ODatabase database = ODatabaseRecordThreadLocal.instance().get();
    database.declareIntent(new OIntentMassiveInsert());

    // Insert File data.
    final long fileSize = file.length();
    final FileInputStream in = new FileInputStream(file);
    try {
      final int CHUNK_SIZE = 81920;
      int bufferedBytes;
      final byte[] buffer = new byte[CHUNK_SIZE];
      byte currentPercent = 0;
      final int fullChunks = (int) (fileSize / CHUNK_SIZE);
      final long fullChunksSize = fullChunks * CHUNK_SIZE;
      final int totalChunks;
      if (fileSize > fullChunksSize) {
        totalChunks = fullChunks + 1;
      } else {
        totalChunks = fullChunks;
      }
      final List<ORID> chunkRids = new ArrayList<ORID>(totalChunks);

      // Make only one ORecordBytes instance and reuse it for every chunk,
      // to reduce heap garbage.
      final ORecordBytes chunk = new ORecordBytes();

      // Handle the full chunks.
      for (int page = 0; page < fullChunks; page++) {
        // Read a full chunk of data from the file into a buffer.
        bufferedBytes = 0;
        while (bufferedBytes < buffer.length) {
          final int bytesRead = in.read(buffer, bufferedBytes, buffer.length - bufferedBytes);
          if (bytesRead == -1) {
            throw new Exception(
                "Reached end of file prematurely. (File changed while reading?) fileName="
                    + file.getAbsolutePath());
          }
          bufferedBytes += bytesRead;
        }

        // Save the chunk to the database.
        final long saveStartTime = System.currentTimeMillis();
        chunk.reset(buffer);
        chunk.save();
        final long saveMs = System.currentTimeMillis() - saveStartTime;

        // Log the amount of time taken by the save.
        System.out.printf("Saved chunk %d in %d ms.\n", page, saveMs);

        // Save the chunk's record ID in the list.
        // Have to copy() the ORID or else every chunk in the list gets the same last ORID.
        // This is because we are using the chunk.reset(); approach to reduce garbage objects.
        chunkRids.add(chunk.getIdentity().copy());

        // Only report progress if it has changed.
        final byte percent = (byte) ((page + 1) * 100 / totalChunks);
        if (percent > currentPercent) {
          System.out.printf("Progress: %d%%\n", percent);
          currentPercent = percent;
        }
      }

      // Handle the final partial chunk (if any).
      if (fullChunks < totalChunks) {
        final int remainder = (int) (fileSize - fullChunksSize);
        // Read the remaining data from the file into a buffer.
        bufferedBytes = 0;
        while (bufferedBytes < remainder) {
          final int bytesRead = in.read(buffer, bufferedBytes, remainder - bufferedBytes);
          if (bytesRead == -1) {
            throw new Exception(
                "Reached end of file prematurely. (File changed while reading?) fileName="
                    + file.getAbsolutePath());
          }
          bufferedBytes += bytesRead;
        }

        // Save the chunk to the database.
        final long saveStartTime = System.currentTimeMillis();
        chunk.reset(Arrays.copyOf(buffer, remainder));
        chunk.save();
        final long saveMs = System.currentTimeMillis() - saveStartTime;

        // Log the amount of time taken by the save.
        System.out.printf("Saved partial chunk %d in %d ms.\n", fullChunks, saveMs);

        // Save the chunk's record ID in the list.
        chunkRids.add(chunk.getIdentity());
      }

      // Should be no more data, so validate this.
      final int b = in.read();
      if (b != -1) {
        throw new Exception(
            "File changed while saving to database! fileName=" + file.getAbsolutePath());
      }

      // Report 100% progress if we haven't already.
      if (currentPercent < 100) {
        System.out.println("Progress: 100%");
      }

      // Save the list of chunk references.
      final long saveChunkListStartTime = System.currentTimeMillis();
      fileDoc.field("DataChunks", chunkRids);
      fileDoc.save();
      final long saveChunkListMs = System.currentTimeMillis() - saveChunkListStartTime;

      // Log the amount of time taken to save the list of chunk RIDs.
      System.out.printf(
          "Saved list of %d chunk RIDs in %d ms.\n", chunkRids.size(), saveChunkListMs);
    } finally {
      database.declareIntent(null);
      in.close();
    }
  }
}
