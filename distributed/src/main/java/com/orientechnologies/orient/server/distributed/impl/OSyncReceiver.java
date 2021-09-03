package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.impl.task.OCopyDatabaseChunkTask;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;

public class OSyncReceiver implements Runnable {
  private ODistributedPlugin distributed;
  private final String databaseName;
  private final ODistributedDatabaseChunk firstChunk;
  private final String iNode;
  private final String dbPath;
  private final CountDownLatch done = new CountDownLatch(1);
  private final CountDownLatch started = new CountDownLatch(1);
  private PipedOutputStream output;
  private PipedInputStream inputStream;
  private volatile boolean finished = false;

  public OSyncReceiver(
      ODistributedPlugin distributed,
      String databaseName,
      ODistributedDatabaseChunk firstChunk,
      String iNode,
      String dbPath) {
    this.distributed = distributed;
    this.databaseName = databaseName;
    this.firstChunk = firstChunk;
    this.iNode = iNode;
    this.dbPath = dbPath;
  }

  public void spawnReceiverThread() {
    try {
      Thread t = new Thread(this);
      t.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      t.start();
    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          iNode,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on transferring database '%s' ",
          e,
          databaseName);
      throw OException.wrapException(
          new ODistributedException("Error on transferring database"), e);
    }
  }

  @Override
  public void run() {
    try {
      Thread.currentThread()
          .setName(
              "OrientDB installDatabase node="
                  + distributed.getLocalNodeName()
                  + " db="
                  + databaseName);
      ODistributedDatabaseChunk chunk = firstChunk;

      output = new PipedOutputStream();
      inputStream = new PipedInputStream(output);
      started.countDown();
      try {

        long fileSize = writeDatabaseChunk(1, chunk, output);
        for (int chunkNum = 2; !chunk.last && !finished; chunkNum++) {
          final ODistributedResponse response =
              distributed.sendRequest(
                  databaseName,
                  null,
                  OMultiValue.getSingletonList(iNode),
                  new OCopyDatabaseChunkTask(
                      chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
                  distributed.getNextMessageIdCounter(),
                  ODistributedRequest.EXECUTION_MODE.RESPONSE,
                  null);

          if (response == null) {
            output.close();
            done.countDown();
            return;
          } else {
            final Object result = response.getPayload();
            if (result instanceof Boolean) continue;
            else if (result instanceof Exception) {
              ODistributedServerLog.error(
                  this,
                  distributed.getLocalNodeName(),
                  iNode,
                  ODistributedServerLog.DIRECTION.IN,
                  "error on installing database %s in %s (chunk #%d)",
                  (Exception) result,
                  databaseName,
                  dbPath,
                  chunkNum);
            } else if (result instanceof ODistributedDatabaseChunk) {
              chunk = (ODistributedDatabaseChunk) result;
              fileSize += writeDatabaseChunk(chunkNum, chunk, output);
            }
          }
        }

        ODistributedServerLog.info(
            this,
            distributed.getLocalNodeName(),
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Database copied correctly, size=%s",
            OFileUtils.getSizeAsString(fileSize));

      } finally {
        try {
          output.flush();
          output.close();
          done.countDown();
        } catch (IOException e) {
          ODistributedServerLog.warn(
              this,
              distributed.getLocalNodeName(),
              null,
              ODistributedServerLog.DIRECTION.NONE,
              "Error on closing sync piped stream ",
              e);
        }
      }

    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          distributed.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on transferring database '%s' ",
          e,
          databaseName);
      throw OException.wrapException(
          new ODistributedException("Error on transferring database"), e);
    }
  }

  protected long writeDatabaseChunk(
      final int iChunkId, final ODistributedDatabaseChunk chunk, final OutputStream out)
      throws IOException {

    ODistributedServerLog.info(
        this,
        distributed.getLocalNodeName(),
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "- writing chunk #%d offset=%d size=%s",
        iChunkId,
        chunk.offset,
        OFileUtils.getSizeAsString(chunk.buffer.length));
    try {
      out.write(chunk.buffer);
    } catch (IOException e) {
      // IN CASE OF ZIP BACKUPS WE CAN IGNORE THE IOException ad the end of the file.
      if (chunk.incremental) {
        throw e;
      }
    }

    return chunk.buffer.length;
  }

  public CountDownLatch getStarted() {
    return started;
  }

  public PipedInputStream getInputStream() {
    return inputStream;
  }

  public CountDownLatch getDone() {
    return done;
  }

  public void close() {
    try {
      finished = true;
      inputStream.close();
    } catch (IOException e) {
      // Ignore
    }
  }
}
