package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.core.storage.impl.local.OSyncSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.*;

public class OBackgroundBackup implements Runnable, OSyncSource {
  private final    TimerTask                             timer;
  private          OSyncDatabaseTask                     oSyncDatabaseTask;
  private final    ODistributedServerManager             iManager;
  private final    ODatabaseDocumentInternal             database;
  private final    File                                  resultedBackupFile;
  private final    String                                finalBackupPath;
  private final    AtomicBoolean                         incremental = new AtomicBoolean(false);
  private final    AtomicReference<ODistributedMomentum> momentum;
  private final    ODistributedDatabase                  dDatabase;
  private final    ODistributedRequestId                 requestId;
  private final    CountDownLatch                        started     = new CountDownLatch(1);
  private final    CountDownLatch                        finished    = new CountDownLatch(1);
  private volatile InputStream                           inputStream;
  public volatile  boolean                               valid       = true;
  private volatile long                                  lastRequest;

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
    lastRequest = System.currentTimeMillis();
    long time = database.getConfiguration().getValueAsLong(DISTRIBUTED_CHECK_HEALTH_EVERY) / 3;
    long maxWait = database.getConfiguration().getValueAsLong(DISTRIBUTED_DEPLOYCHUNK_TASK_SYNCH_TIMEOUT) * 3;
    timer = Orient.instance().scheduleTask(() -> {
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastRequest > maxWait) {
        try {
          inputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        finished.countDown();
        valid = false;
      }
    }, time, time);
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

        if (resultedBackupFile.exists())
          resultedBackupFile.delete();
        else
          resultedBackupFile.getParentFile().mkdirs();
        resultedBackupFile.createNewFile();

        final OutputStream fileOutputStream = new FileOutputStream(resultedBackupFile);
        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        inputStream = new PipedInputStream(pipedOutputStream, OSyncDatabaseTask.CHUNK_MAX_SIZE);
        OutputStream dest = new TeeOutputStream(fileOutputStream, pipedOutputStream);
        if (database.getStorage().supportIncremental()) {
          OWriteAheadLog wal = ((OAbstractPaginatedStorage) database.getStorage().getUnderlying()).getWALInstance();
          OLogSequenceNumber lsn = wal.end();
          if (lsn == null) {
            lsn = new OLogSequenceNumber(-1, -1);
          }
          wal.addCutTillLimit(lsn);

          try {
            incremental.set(true);
            started.countDown();
            database.getStorage().fullIncrementalBackup(dest);
          } catch (UnsupportedOperationException u) {
            throw u;
          } catch (RuntimeException r) {
            finished.countDown();
            timer.cancel();
            throw r;
          } finally {
            wal.removeCutTillLimit(lsn);
          }
          finished.countDown();
          timer.cancel();
          OLogManager.instance().info(this, "Sending Enterprise backup (" + database.getName() + ") for node sync");

        } else {
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
            database.backup(dest, null, () -> {
                  momentum.set(dDatabase.getSyncConfiguration().getMomentum().copy());
                  incremental.set(false);
                  started.countDown();
                  return null;
                }, listener, OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger(),
                OAbstractSyncDatabaseTask.CHUNK_MAX_SIZE);
          } finally {
            try {
              dest.close();
            } catch (IOException e2) {
              OLogManager.instance().debug(this, "Error performing backup ", e2);
            }
            finished.countDown();
            timer.cancel();
          }
        }

        ODistributedServerLog
            .info(this, iManager.getLocalNodeName(), oSyncDatabaseTask.getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
                "Backup of database '%s' completed. lastOperationId=%s...", database.getName(), requestId);

      } catch (Exception e) {
        OLogManager.instance().error(this, "Cannot execute backup of database '%s' for deploy database", e, database.getName());
        throw e;
      } finally {
        finished.countDown();
        timer.cancel();
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

  public void makeStreamFromFile() throws IOException, InterruptedException {
    getFinished().await();
    inputStream = new FileInputStream(finalBackupPath);
  }

  public boolean getIncremental() {
    return incremental.get();
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

  public InputStream getInputStream() {
    lastRequest = System.currentTimeMillis();
    return inputStream;
  }

  @Override
  public void invalidate() {
    valid = false;
  }

  @Override
  public boolean isValid() {
    return valid;
  }
}
