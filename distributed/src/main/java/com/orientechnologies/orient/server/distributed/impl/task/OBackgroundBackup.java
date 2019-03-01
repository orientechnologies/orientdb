package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedMomentum;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class OBackgroundBackup implements Runnable {
  private       OSyncDatabaseTask                     oSyncDatabaseTask;
  private final ODistributedServerManager             iManager;
  private final ODatabaseDocumentInternal             database;
  private final File                                  resultedBackupFile;
  private final String                                finalBackupPath;
  private final AtomicBoolean                         incremental = new AtomicBoolean(false);
  private final AtomicReference<ODistributedMomentum> momentum;
  private final ODistributedDatabase                  dDatabase;
  private final ODistributedRequestId                 requestId;
  private final File                                  completedFile;
  private       CountDownLatch                        started     = new CountDownLatch(1);
  private       CountDownLatch                        finished    = new CountDownLatch(1);

  public OBackgroundBackup(OSyncDatabaseTask oSyncDatabaseTask, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database, File resultedBackupFile, String finalBackupPath, OModifiableBoolean incremental,
      AtomicReference<ODistributedMomentum> momentum, ODistributedDatabase dDatabase, ODistributedRequestId requestId,
      File completedFile) {
    this.oSyncDatabaseTask = oSyncDatabaseTask;
    this.iManager = iManager;
    this.database = database;
    this.resultedBackupFile = resultedBackupFile;
    this.finalBackupPath = finalBackupPath;
    this.momentum = momentum;
    this.dDatabase = dDatabase;
    this.requestId = requestId;
    this.completedFile = completedFile;
  }

  @Override
  public void run() {
    Thread.currentThread().setName("OrientDB SyncDatabase node=" + iManager.getLocalNodeName() + " db=" + database.getName());

    try {
      try {
        database.activateOnCurrentThread();

        ODistributedServerLog
            .info(this, iManager.getLocalNodeName(), oSyncDatabaseTask.getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
                "Compressing database '%s' %d clusters %s...", database.getName(), database.getClusterNames().size(),
                database.getClusterNames());

        if (database.getStorage().supportIncremental()) {
          OWriteAheadLog wal = ((OAbstractPaginatedStorage) database.getStorage().getUnderlying()).getWALInstance();
          OLogSequenceNumber lsn = wal.end();
          if (lsn == null) {
            lsn = new OLogSequenceNumber(-1, -1);
          }
          wal.addCutTillLimit(lsn);

          resultedBackupFile.delete();

          try {
            final File incrementalFile = new File(finalBackupPath);
            incrementalFile.createNewFile();
            incremental.set(true);
            started.countDown();
            database.getStorage().fullIncrementalBackup(new FileOutputStream(incrementalFile));
          } catch (UnsupportedOperationException u) {
            throw u;
          } catch (RuntimeException r) {
            finished.countDown();
            throw r;
          } finally {
            wal.removeCutTillLimit(lsn);
          }
          finished.countDown();
          OLogManager.instance().info(this, "Sending Enterprise backup (" + database.getName() + ") for node sync");

        } else {

          if (resultedBackupFile.exists())
            resultedBackupFile.delete();
          else
            resultedBackupFile.getParentFile().mkdirs();
          resultedBackupFile.createNewFile();

          final FileOutputStream fileOutputStream = new FileOutputStream(resultedBackupFile);
          try {
            OCommandOutputListener listener = null;
            if (ODistributedServerLog.isDebugEnabled()) {
              listener = new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                  if (iText.startsWith("\n"))
                    iText = iText.substring(1);

                  OLogManager.instance().debug(this, iText);
                }
              };
            }
            database.backup(fileOutputStream, null, () -> {
                  momentum.set(dDatabase.getSyncConfiguration().getMomentum().copy());
                  incremental.set(false);
                  started.countDown();
                  return null;
                }, listener, OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger(),
                OAbstractSyncDatabaseTask.CHUNK_MAX_SIZE);
          } finally {
            try {
              fileOutputStream.close();
            } catch (IOException e2) {
              OLogManager.instance().debug(this, "Error performing backup ", e2);
            }

          }
        }

        ODistributedServerLog
            .info(this, iManager.getLocalNodeName(), oSyncDatabaseTask.getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
                "Backup of database '%s' completed. lastOperationId=%s...", database.getName(), requestId);

      } catch (Exception e) {
        OLogManager.instance().error(this, "Cannot execute backup of database '%s' for deploy database", e, database.getName());
        throw e;
      } finally {
        try {
          completedFile.createNewFile();
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot create file of backup completed: %s", e, completedFile);
        }
        finished.countDown();
      }
    } catch (Exception e) {
      OLogManager.instance().errorNoDb(this, "Error during backup processing, file %s will be deleted\n", e, resultedBackupFile);
      try {
        Files.deleteIfExists(Paths.get(resultedBackupFile.getAbsolutePath()));
      } catch (IOException ioe) {
        OLogManager.instance().errorNoDb(this, "Can not delete file %s\n", ioe, resultedBackupFile);
      }
    }

  }

  public AtomicBoolean getIncremental() {
    return incremental;
  }

  public File getResultedBackupFile() {
    return resultedBackupFile;
  }

  public String getFinalBackupPath() {
    return finalBackupPath;
  }

  public CountDownLatch getStarted() {
    return started;
  }

  public CountDownLatch getFinished() {
    return finished;
  }
}
