package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OSyncSource;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class OBackgroundBackup implements Runnable, OSyncSource {
  private OSyncDatabaseTask oSyncDatabaseTask;
  private final ODistributedServerManager iManager;
  private final ODatabaseDocumentInternal database;
  private final File resultedBackupFile;
  private final String finalBackupPath;
  private final AtomicBoolean incremental = new AtomicBoolean(false);
  private final ODistributedDatabase dDatabase;
  private final ODistributedRequestId requestId;
  private final CountDownLatch started = new CountDownLatch(1);
  private final CountDownLatch finished = new CountDownLatch(1);
  private volatile InputStream inputStream;
  public volatile boolean valid = true;
  private TimerTask timerTask;
  private volatile long lastRead;

  public OBackgroundBackup(
      OSyncDatabaseTask oSyncDatabaseTask,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database,
      File resultedBackupFile,
      String finalBackupPath,
      OModifiableBoolean incremental,
      ODistributedDatabase dDatabase,
      ODistributedRequestId requestId,
      File completedFile) {
    this.oSyncDatabaseTask = oSyncDatabaseTask;
    this.iManager = iManager;
    this.database = database;
    this.resultedBackupFile = resultedBackupFile;
    this.finalBackupPath = finalBackupPath;
    this.dDatabase = dDatabase;
    this.requestId = requestId;
  }

  @Override
  public void run() {
    Thread.currentThread()
        .setName(
            "OrientDB SyncDatabase node="
                + iManager.getLocalNodeName()
                + " db="
                + database.getName());
    database.activateOnCurrentThread();
    startExpireTask();
    try {
      try {

        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            oSyncDatabaseTask.getNodeSource(),
            ODistributedServerLog.DIRECTION.OUT,
            "Compressing database '%s' %d clusters %s...",
            database.getName(),
            database.getClusterNames().size(),
            database.getClusterNames());

        if (resultedBackupFile.exists()) resultedBackupFile.delete();
        else resultedBackupFile.getParentFile().mkdirs();
        resultedBackupFile.createNewFile();

        final OutputStream fileOutputStream = new FileOutputStream(resultedBackupFile);
        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        inputStream = new PipedInputStream(pipedOutputStream, OSyncDatabaseTask.CHUNK_MAX_SIZE);
        OutputStream dest = new TeeOutputStream(fileOutputStream, pipedOutputStream);
        if (database.getStorage().supportIncremental()) {
          OWriteAheadLog wal = ((OAbstractPaginatedStorage) database.getStorage()).getWALInstance();
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
            throw r;
          } finally {
            wal.removeCutTillLimit(lsn);
          }
          finished.countDown();
          OLogManager.instance()
              .info(this, "Sending Enterprise backup (" + database.getName() + ") for node sync");

        } else {
          try {
            OCommandOutputListener listener = null;
            if (ODistributedServerLog.isDebugEnabled()) {
              listener =
                  new OCommandOutputListener() {
                    @Override
                    public void onMessage(String iText) {
                      if (iText.startsWith("\n")) iText = iText.substring(1);

                      OLogManager.instance().debug(this, iText);
                    }
                  };
            }
            int compression =
                OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger();
            Callable<Object> startListener =
                () -> {
                  incremental.set(false);
                  started.countDown();
                  return null;
                };
            database.backup(
                dest, null, startListener, listener, compression, OSyncDatabaseTask.CHUNK_MAX_SIZE);
          } finally {
            try {
              dest.close();
            } catch (IOException e2) {
              OLogManager.instance().debug(this, "Error performing backup ", e2);
            }
            finished.countDown();
            this.timerTask.cancel();
          }
        }

        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            oSyncDatabaseTask.getNodeSource(),
            ODistributedServerLog.DIRECTION.OUT,
            "Backup of database '%s' completed. lastOperationId=%s...",
            database.getName(),
            requestId);

      } catch (Exception e) {
        OLogManager.instance()
            .error(
                this,
                "Cannot execute backup of database '%s' for deploy database",
                e,
                database.getName());
        throw e;
      } finally {
        finished.countDown();
        this.timerTask.cancel();
      }
    } catch (Exception e) {
      OLogManager.instance()
          .errorNoDb(
              this,
              "Error during backup processing, file %s will be deleted\n",
              e,
              resultedBackupFile);
      try {
        Files.deleteIfExists(Paths.get(resultedBackupFile.getAbsolutePath()));
      } catch (IOException ioe) {
        OLogManager.instance().errorNoDb(this, "Can not delete file %s\n", ioe, resultedBackupFile);
      }
    }
  }

  private void startExpireTask() {
    lastRead = System.currentTimeMillis();
    long timeout =
        database
            .getConfiguration()
            .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DEPLOYCHUNK_TASK_SYNCH_TIMEOUT);
    timerTask =
        new TimerTask() {
          @Override
          public void run() {
            if (System.currentTimeMillis() - lastRead > timeout * 3) {
              try {
                inputStream.close();
                this.cancel();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        };
    database.getSharedContext().getOrientDB().schedule(timerTask, timeout, timeout);
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
    this.lastRead = System.currentTimeMillis();
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
