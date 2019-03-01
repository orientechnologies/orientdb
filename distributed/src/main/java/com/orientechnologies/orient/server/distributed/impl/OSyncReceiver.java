package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedMomentum;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.impl.task.OCopyDatabaseChunkTask;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class OSyncReceiver implements Runnable {
  private       ODistributedAbstractPlugin            distributed;
  private final String                                databaseName;
  private final ODistributedDatabaseChunk             firstChunk;
  private final AtomicReference<ODistributedMomentum> momentum;
  private final String                                iNode;
  private final String                                dbPath;
  private final CountDownLatch                        done    = new CountDownLatch(1);
  private final CountDownLatch                        started = new CountDownLatch(1);
  private       PipedOutputStream                     output;
  private       PipedInputStream                      inputStream;

  public OSyncReceiver(ODistributedAbstractPlugin distributed, String databaseName, ODistributedDatabaseChunk firstChunk,
      AtomicReference<ODistributedMomentum> momentum, String iNode, String dbPath) {
    this.distributed = distributed;
    this.databaseName = databaseName;
    this.firstChunk = firstChunk;
    this.momentum = momentum;
    this.iNode = iNode;
    this.dbPath = dbPath;
  }

  @Override
  public void run() {
    try {
      Thread.currentThread().setName("OrientDB installDatabase node=" + distributed.nodeName + " db=" + databaseName);
      ODistributedDatabaseChunk chunk = firstChunk;

      momentum.set(chunk.getMomentum());

      output = new PipedOutputStream();
      inputStream = new PipedInputStream(output);
      started.countDown();
      try {

        long fileSize = distributed.writeDatabaseChunk(1, chunk, output);
        for (int chunkNum = 2; !chunk.last; chunkNum++) {
          final ODistributedResponse response = distributed.sendRequest(databaseName, null, OMultiValue.getSingletonList(iNode),
              new OCopyDatabaseChunkTask(chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
              distributed.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

          final Object result = response.getPayload();
          if (result instanceof Boolean)
            continue;
          else if (result instanceof Exception) {
            ODistributedServerLog.error(this, distributed.nodeName, iNode, ODistributedServerLog.DIRECTION.IN,
                "error on installing database %s in %s (chunk #%d)", (Exception) result, databaseName, dbPath, chunkNum);
          } else if (result instanceof ODistributedDatabaseChunk) {
            chunk = (ODistributedDatabaseChunk) result;
            fileSize += distributed.writeDatabaseChunk(chunkNum, chunk, output);
          }
        }

        output.flush();
        done.countDown();

        ODistributedServerLog
            .info(this, distributed.nodeName, null, ODistributedServerLog.DIRECTION.NONE, "Database copied correctly, size=%s",
                OFileUtils.getSizeAsString(fileSize));

      } finally {
        try {
          output.flush();
          output.close();
        } catch (IOException e) {
          ODistributedServerLog
              .warn(this, distributed.nodeName, null, ODistributedServerLog.DIRECTION.NONE, "Error on closing sync piped stream ",
                  e);

        }
      }

    } catch (Exception e) {
      ODistributedServerLog
          .error(this, distributed.nodeName, null, ODistributedServerLog.DIRECTION.NONE, "Error on transferring database '%s' ", e,
              databaseName);
      throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
    }
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
}
