/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

public class ODistributedDatabaseChunk implements DataSerializable {
  public long               lastOperationId;
  public String             filePath;
  public long               offset;
  public byte[]             buffer;
  public OLogSequenceNumber lsn;
  public boolean            gzipCompressed;
  public boolean            last;

  public ODistributedDatabaseChunk() {
  }

  public ODistributedDatabaseChunk(final long iLastOperationId, final File iFile, final long iOffset, final int iMaxSize,
      final OLogSequenceNumber iLSN, final boolean gzipCompressed) throws IOException {
    lastOperationId = iLastOperationId;
    filePath = iFile.getAbsolutePath();
    offset = iOffset;
    lsn = iLSN;
    this.gzipCompressed = gzipCompressed;

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

  public long getLastOperationId() {
    return lastOperationId;
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    out.writeLong(lastOperationId);
    out.writeUTF(filePath);
    out.writeLong(offset);
    out.writeInt(buffer.length);
    out.write(buffer);
    out.writeBoolean(lsn != null);
    if (lsn != null) {
      out.writeLong(lsn.getSegment());
      out.writeLong(lsn.getPosition());
    }
    out.writeBoolean(gzipCompressed);
    out.writeBoolean(last);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    lastOperationId = in.readLong();
    filePath = in.readUTF();
    offset = in.readLong();
    final int size = in.readInt();
    buffer = new byte[size];
    in.readFully(buffer);
    final boolean lsnNotNull = in.readBoolean();
    lsn = lsnNotNull ? new OLogSequenceNumber(in.readLong(), in.readLong()) : null;
    gzipCompressed = in.readBoolean();
    last = in.readBoolean();
  }
}
