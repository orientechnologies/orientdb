package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.OCopyDatabaseChunkTask;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class OSyncReceiver implements Runnable {
  private       ODistributedAbstractPlugin            distributed;
  private final String                                databaseName;
  private final ODistributedDatabaseChunk             firstChunk;
  private final AtomicReference<ODistributedMomentum> momentum;
  private final String                                fileName;
  private final String                                iNode;
  private final String                                dbPath;
  private final File                                  file;
  private final CountDownLatch                        done = new CountDownLatch(1);

  public OSyncReceiver(ODistributedAbstractPlugin distributed, String databaseName, ODistributedDatabaseChunk firstChunk,
      AtomicReference<ODistributedMomentum> momentum, String fileName, String iNode, String dbPath, File file) {
    this.distributed = distributed;
    this.databaseName = databaseName;
    this.firstChunk = firstChunk;
    this.momentum = momentum;
    this.fileName = fileName;
    this.iNode = iNode;
    this.dbPath = dbPath;
    this.file = file;
  }

  @Override
  public void run() {
    try {
      Thread.currentThread().setName("OrientDB installDatabase node=" + distributed.nodeName + " db=" + databaseName);
      ODistributedDatabaseChunk chunk = firstChunk;

      momentum.set(chunk.getMomentum());

      final OutputStream fOut = new FileOutputStream(fileName, false);
      try {

        long fileSize = distributed.writeDatabaseChunk(1, chunk, fOut);
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
            fileSize += distributed.writeDatabaseChunk(chunkNum, chunk, fOut);
          }
        }

        fOut.flush();
        done.countDown();

        // CREATE THE .COMPLETED FILE TO SIGNAL EOF
        new File(file.getAbsolutePath() + ".completed").createNewFile();

        ODistributedServerLog
            .info(this, distributed.nodeName, null, ODistributedServerLog.DIRECTION.NONE, "Database copied correctly, size=%s",
                OFileUtils.getSizeAsString(fileSize));

      } finally {
        try {
          fOut.flush();
          fOut.close();
        } catch (IOException e) {
        }
      }

    } catch (Exception e) {
      ODistributedServerLog.error(this, distributed.nodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "Error on transferring database '%s' to '%s'", e, databaseName, fileName);
      throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
    }
  }

  public CountDownLatch getLatch() {
    return done;
  }
}
