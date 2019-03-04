/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.distributed.ODistributedMomentum;
import com.orientechnologies.orient.server.distributed.impl.task.OBackgroundBackup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class ODistributedDatabaseChunk implements OStreamable {
  public  String               filePath;
  public  long                 offset;
  public  byte[]               buffer;
  public  boolean              gzipCompressed;
  public  boolean              last;
  private ODistributedMomentum momentum;
  public  boolean              incremental;
  //This are not used anymore remove in the next version
  public  long                 walSegment;
  public  long                 walPosition;

  public ODistributedDatabaseChunk() {
  }

  public ODistributedDatabaseChunk(final OBackgroundBackup backgroundBackup, final long iOffset, final int iMaxSize,
      final ODistributedMomentum momentum) throws IOException {
    filePath = "";
    offset = iOffset;
    this.momentum = momentum;
    this.gzipCompressed = false;
    this.incremental = backgroundBackup.getIncremental().get();
    this.walSegment = -1;
    this.walPosition = -1;

    try {
      final InputStream in = backgroundBackup.getInputStream();
      byte[] local = new byte[iMaxSize];
      int read = 0;
      read = in.read(local);
      if (read == -1) {
        buffer = new byte[] {};
        last = true;
      } else {
        buffer = new byte[read];
        System.arraycopy(local, 0, buffer, 0, read);
        // UPDATE FILE SIZE

        if (in.available() == 0 && backgroundBackup.getFinished().await(0, TimeUnit.NANOSECONDS)) {
          // BACKUP COMPLETED
          last = true;
        }
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
    }

  }

  public ODistributedDatabaseChunk(final File iFile, final long iOffset, final int iMaxSize, final ODistributedMomentum momentum,
      final boolean gzipCompressed, boolean incremental) throws IOException {
    this(iFile, iOffset, iMaxSize, momentum, gzipCompressed, incremental, -1, -1);
  }

  public ODistributedDatabaseChunk(final File iFile, final long iOffset, final int iMaxSize, final ODistributedMomentum momentum,
      final boolean gzipCompressed, boolean incremental, long walSegment, long walPosition) throws IOException {
    filePath = iFile.getAbsolutePath();
    offset = iOffset;
    this.momentum = momentum;
    this.gzipCompressed = gzipCompressed;
    this.incremental = incremental;
    this.walSegment = walSegment;
    this.walPosition = walPosition;

    long fileSize = iFile.length();

    final File completedFile = new File(iFile.getAbsolutePath() + ".completed");

    // WHILE UNTIL THE CHUNK IS AVAILABLE
    for (int retry = 0; fileSize <= iOffset; ++retry) {
      if (fileSize == 0 || iOffset > fileSize)
        try {
          // WAIT FOR ASYNCH WRITE
          Thread.sleep(300);
        } catch (InterruptedException e) {
        }

      // UPDATE FILE SIZE
      fileSize = iFile.length();

      if (completedFile.exists())
        // BACKUP FINISHED
        break;
    }

    final int toRead = (int) Math.min(iMaxSize, fileSize - offset);
    buffer = new byte[toRead];

    final InputStream in = gzipCompressed ? new GZIPInputStream(new FileInputStream(iFile)) : new FileInputStream(iFile);
    try {
      in.skip(offset);
      in.read(buffer);

    } finally {
      try {
        in.close();
      } catch (IOException e) {
      }
    }

    // UPDATE FILE SIZE
    fileSize = iFile.length();

    if (completedFile.exists() && fileSize - (offset + toRead) == 0) {
      // BACKUP COMPLETED
      last = true;
    }
  }

  @Override
  public String toString() {
    return filePath + "[" + offset + "-" + buffer.length + "] (last=" + last + ")";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeUTF(filePath);
    out.writeLong(offset);
    out.writeInt(buffer.length);
    out.write(buffer);
    if (momentum != null) {
      out.writeBoolean(true);
      momentum.toStream(out);
    } else
      out.writeBoolean(false);

    out.writeBoolean(gzipCompressed);
    out.writeBoolean(last);
    out.writeBoolean(incremental);
    out.writeLong(walSegment);
    out.writeLong(walPosition);
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    filePath = in.readUTF();
    offset = in.readLong();
    int size = in.readInt();
    buffer = new byte[size];
    in.readFully(buffer);

    momentum = new ODistributedMomentum();
    final boolean momentumPresent = in.readBoolean();
    if (momentumPresent) {
      momentum.fromStream(in);
    }

    gzipCompressed = in.readBoolean();
    last = in.readBoolean();
    incremental = in.readBoolean();
    walSegment = in.readLong();
    walPosition = in.readLong();
  }

  public ODistributedMomentum getMomentum() {
    return momentum;
  }

  public OLogSequenceNumber getLastWal() {
    return new OLogSequenceNumber(walSegment, walPosition);
  }

}
