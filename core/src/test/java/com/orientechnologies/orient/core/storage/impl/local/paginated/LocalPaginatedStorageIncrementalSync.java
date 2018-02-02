package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Random;

public class LocalPaginatedStorageIncrementalSync {
  private ODatabaseDocumentTx originalDB;
  private ODatabaseDocumentTx syncDB;

  @After
  public void afterMethod() {
    originalDB.activateOnCurrentThread();
    originalDB.drop();

    syncDB.activateOnCurrentThread();
    syncDB.drop();
  }

  @Test
  public void testIncrementalSynch() throws Exception {
    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);

    final String buildDirectory = System.getProperty("buildDirectory", ".");

    createOriginalDB(buildDirectory);
    createSyncDB(buildDirectory);

    assertDatabasesAreInSynch();

    for (int i = 0; i < 10; i++) {
      System.out.println("Iteration " + (i + 1) + "-----------------------------------------------");
      incrementalSyncIteration(buildDirectory);
    }

  }

  private void incrementalSyncIteration(String buildDirectory) throws Exception {
    OLogSequenceNumber startLSN = ((OAbstractPaginatedStorage) originalDB.getStorage()).getWALInstance().end();
    final Random rnd = new Random();

    int created = 0;
    int updated = 0;
    int deleted = 0;

    while (created + updated + deleted < 10000) {
      final int operation = rnd.nextInt(3);
      switch (operation) {
      case 0:
        createRecord(rnd);
        created++;
        break;
      case 1:
        if (updateRecord(rnd))
          updated++;
        break;
      case 2:
        if (deleteRecord(rnd))
          deleted++;
        break;
      }
    }

    System.out.println("Created " + created);
    System.out.println("Updated " + updated);
    System.out.println("Deleted " + deleted);

    final File changesFile = new File(buildDirectory, LocalPaginatedStorageIncrementalSync.class.getSimpleName() + ".dt");

    if (changesFile.exists()) {
      Assert.assertTrue(changesFile.delete());
    }

    RandomAccessFile dataFile = new RandomAccessFile(changesFile, "rw");
    try {
      FileChannel channel = dataFile.getChannel();
      final OutputStream outputStream = Channels.newOutputStream(channel);
      final OutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

      ((OAbstractPaginatedStorage) originalDB.getStorage()).recordsChangedAfterLSN(startLSN, bufferedOutputStream, null);
      bufferedOutputStream.close();

      dataFile.close();
      dataFile = new RandomAccessFile(changesFile, "rw");

      channel = dataFile.getChannel();

      final InputStream inputStream = Channels.newInputStream(channel);
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

      replicateDelta(syncDB, bufferedInputStream);
    } finally {
      dataFile.close();
    }

    assertDatabasesAreInSynch();

    Assert.assertTrue(changesFile.delete());
  }

  private void replicateDelta(ODatabaseDocumentTx db, InputStream in) throws Exception {
    db.activateOnCurrentThread();

    final DataInputStream input = new DataInputStream(in);
    try {
      final long records = input.readLong();

      for (long i = 0; i < records; ++i) {
        final int clusterId = input.readInt();
        final long clusterPos = input.readLong();
        final boolean deleted = input.readBoolean();

        final ORecordId rid = new ORecordId(clusterId, clusterPos);

        final ORecord loadedRecord = rid.getRecord();

        if (deleted) {

          if (loadedRecord != null)
            // DELETE IT
            db.delete(rid);

        } else {
          final int recordVersion = input.readInt();
          final int recordType = input.readByte();
          final int recordSize = input.readInt();
          final byte[] recordContent = new byte[recordSize];

          int bytesRead = 0;
          while (bytesRead < recordContent.length) {
            final int ac = input.read(recordContent);

            if (ac == -1)
              throw new IllegalStateException("Unexpected end of stream is reached");

            bytesRead += ac;
          }

          ORecord newRecord = null;

          if (loadedRecord == null) {
            do {
              newRecord = Orient.instance().getRecordFactoryManager().newInstance((byte) recordType, clusterId, db);

              ORecordInternal.fill(newRecord, new ORecordId(rid.getClusterId(), -1), recordVersion - 1, recordContent, true);

              newRecord.save();

              if (newRecord.getIdentity().getClusterPosition() < clusterPos) {
                // DELETE THE RECORD TO CREATE A HOLE
                newRecord.delete();
              }

            } while (newRecord.getIdentity().getClusterPosition() < clusterPos);

          } else {
            // UPDATE
            newRecord = Orient.instance().getRecordFactoryManager().newInstance((byte) recordType, clusterId, db);
            ORecordInternal.fill(newRecord, rid, ORecordVersionHelper.setRollbackMode(recordVersion), recordContent, true);

            if (loadedRecord instanceof ODocument) {
              // APPLY CHANGES FIELD BY FIELD TO MARK DIRTY FIELDS FOR INDEXES/HOOKS
              ODocument loadedDocument = (ODocument) loadedRecord;
              loadedDocument.merge((ODocument) newRecord, false, false).getVersion();
              loadedDocument.setDirty();
              newRecord = loadedDocument;
            }

            // SAVE THE UPDATE RECORD
            newRecord.save();
          }
        }
      }

      db.getMetadata().reload();
    } finally {
      input.close();
    }

  }

  private boolean updateRecord(Random random) {
    originalDB.activateOnCurrentThread();

    final int clusterId = originalDB.getClusterIdByName("Sample");
    final long[] rids = originalDB.getStorage().getClusterDataRange(clusterId);
    boolean updated = false;

    if (rids[0] == -1)
      return false;

    while (!updated) {
      final int deleteIndex = random.nextInt(rids.length);
      final ORecordId recordId = new ORecordId(clusterId, rids[deleteIndex]);
      try {
        final ODocument document = originalDB.load(recordId);
        if (document != null) {
          final byte[] data = new byte[256];
          random.nextBytes(data);

          document.field("data", data);
          document.save();
          updated = true;
        }
      } catch (ORecordNotFoundException e) {
        updated = false;
      }
    }

    return true;
  }

  private boolean deleteRecord(Random random) {
    originalDB.activateOnCurrentThread();

    final int clusterId = originalDB.getClusterIdByName("Sample");
    final long[] rids = originalDB.getStorage().getClusterDataRange(clusterId);
    if (rids[0] == -1)
      return false;

    boolean deleted = false;

    while (!deleted) {
      final int deleteIndex = random.nextInt(rids.length);
      final ORecordId recordId = new ORecordId(clusterId, rids[deleteIndex]);
      try {
        final ODocument document = originalDB.load(recordId);
        if (document != null) {
          document.delete();
          deleted = true;
        }
      } catch (ORecordNotFoundException e) {
        deleted = false;
      }
    }

    return true;
  }

  private void createRecord(Random random) {
    originalDB.activateOnCurrentThread();

    final ODocument document = new ODocument("Sample");
    final byte[] data = new byte[256];
    random.nextBytes(data);

    document.field("data", data);
    document.save();
  }

  private void createSyncDB(String buildDirectory) {
    syncDB = new ODatabaseDocumentTx(
        "plocal:" + buildDirectory + "/" + LocalPaginatedStorageIncrementalSync.class.getSimpleName() + "Sync");
    createAndInitDatabase(syncDB);
  }

  private void createOriginalDB(String buildDirectory) {
    originalDB = new ODatabaseDocumentTx(
        "plocal:" + buildDirectory + "/" + LocalPaginatedStorageIncrementalSync.class.getSimpleName() + "Original");
    createAndInitDatabase(originalDB);
  }

  private void createAndInitDatabase(ODatabaseDocumentTx database) {
    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();
    OSchema schema = database.getMetadata().getSchema();
    schema.createClass("Sample");
  }

  private void assertDatabasesAreInSynch() throws Exception {
    originalDB.activateOnCurrentThread();
    final long originalRecords = originalDB.countClass("Sample");

    syncDB.activateOnCurrentThread();
    final long syncRecords = syncDB.countClass("Sample");

    Assert.assertEquals(originalRecords, syncRecords);

    originalDB.activateOnCurrentThread();
    OSchema schema = originalDB.getMetadata().getSchema();
    OClass clazz = schema.getClass("Sample");

    int[] clusterIds = clazz.getClusterIds();
    for (int clusterId : clusterIds) {
      final OStorage originalStorage = originalDB.getStorage();
      final OStorage syncedStorage = syncDB.getStorage();

      final long[] db1Range = originalStorage.getClusterDataRange(clusterId);
      final long[] db2Range = syncedStorage.getClusterDataRange(clusterId);

      Assert.assertEquals(db1Range, db2Range);

      final ORecordId rid = new ORecordId(clusterId);
      OPhysicalPosition[] physicalPositions = originalStorage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));

      while (physicalPositions.length > 0) {
        for (OPhysicalPosition physicalPosition : physicalPositions) {
          rid.setClusterPosition(physicalPosition.clusterPosition);
          final ORawBuffer originalBuffer = originalStorage.readRecord(rid, null, true, false, null).getResult();
          final ORawBuffer syncBuffer = syncedStorage.readRecord(rid, null, true, false, null).getResult();

          Assert.assertEquals(originalBuffer.recordType, syncBuffer.recordType);
          Assert.assertEquals(originalBuffer.version, syncBuffer.version);
          Assert.assertEquals(originalBuffer.buffer, syncBuffer.buffer);
        }

        physicalPositions = originalStorage.higherPhysicalPositions(clusterId, physicalPositions[physicalPositions.length - 1]);
      }
    }
  }
}
