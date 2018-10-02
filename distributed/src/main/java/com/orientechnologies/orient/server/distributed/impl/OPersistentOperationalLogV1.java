package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.OrientDBDistributed;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class OPersistentOperationalLogV1 implements OOperationLog {

  private OCoordinateMessagesFactory factory;

  private static class OplogInfo {
    int  currentFileNum;
    int  firstFileNum;
    long keepUntil;

    void fromStream(InputStream stream) {
      try {
        DataInputStream in = new DataInputStream(stream);
        int version = in.readInt();
        if (version != 0) {
          throw new ODistributedException("Wrong oplog info: version " + version);
        }
        currentFileNum = in.readInt();
        firstFileNum = in.readInt();
        keepUntil = in.readLong();
      } catch (IOException e) {
        throw new ODistributedException("Cannot read oplog info: " + e.getMessage());
      }
    }

    void toStream(OutputStream stream) {
      try {
        DataOutputStream out = new DataOutputStream(stream);
        out.writeInt(0);
        out.writeInt(currentFileNum);
        out.writeInt(firstFileNum);
        out.writeLong(keepUntil);
      } catch (Exception e) {
        throw new ODistributedException("Cannot write oplog info: " + e.getMessage());
      }
    }
  }

  protected static final long   MAGIC                = 6148914691236517205L; //101010101010101010101010101010101010101010101010101010101010101
  protected static final String OPLOG_INFO_FILE      = "oplog.opl";
  protected static final String OPLOG_FILE           = "oplog_$NUM$.opl";
  protected static final int    LOG_ENTRIES_PER_FILE = 16 * 1024;

  private final String    storagePath;
  private       OplogInfo info;

  private FileOutputStream fileOutput;
  private DataOutputStream stream;

  private AtomicLong inc;

  public static OPersistentOperationalLogV1 newInstance(String databaseName, OrientDBInternal context) {
    OAbstractPaginatedStorage s = ((OrientDBDistributed) context).getStorage(databaseName);
    OLocalPaginatedStorage storage = (OLocalPaginatedStorage) s.getUnderlying();
    return new OPersistentOperationalLogV1(storage.getStoragePath().toString());
  }

  public OPersistentOperationalLogV1(String storageFolder) {
    this.storagePath = storageFolder;
    this.info = readInfo();
    this.stream = initStream(info);
    inc = readLastLogId();
  }

  private DataOutputStream initStream(OplogInfo info) {
    String filePath = calculateLogFileFullPath(info.currentFileNum);
    try {
      File file = new File(filePath);
      if (!file.exists()) {
        file.createNewFile();
      }
      this.fileOutput = new FileOutputStream(file, true);
      return new DataOutputStream(fileOutput);
    } catch (IOException e) {
      throw new ODistributedException("Cannot create oplog file " + info.currentFileNum + ": " + e.getMessage());
    }
  }

  protected String calculateLogFileFullPath(int fileNum) {
    String fileName = OPLOG_FILE.replace("$NUM$", "" + fileNum);
    String fullPath = storagePath + File.separator + fileName;
    return fullPath;
  }

  private OplogInfo readInfo() {
    File infoFile = new File(storagePath, OPLOG_INFO_FILE);
    OplogInfo result = new OplogInfo();
    if (infoFile.exists()) {
      writeInfo(infoFile, result);
    } else {
      initNewInfoFile(infoFile, result);
    }
    return result;
  }

  private void writeInfo(File infoFile, OplogInfo result) {
    try {
      FileOutputStream stream = new FileOutputStream(infoFile);
      result.toStream(stream);
      stream.close();
    } catch (IOException e) {
      throw new ODistributedException("Cannot write oplog info:" + e.getMessage());
    }
  }

  private void initNewInfoFile(File infoFile, OplogInfo result) {
    try {
      result.currentFileNum = -1;
      result.firstFileNum = 0;
      result.keepUntil = 0;
      infoFile.createNewFile();
      FileOutputStream stream = new FileOutputStream(infoFile);
      result.toStream(stream);
      stream.flush();
      stream.close();
    } catch (IOException e) {
      throw new ODistributedException("Cannot init oplog info:" + e.getMessage());
    }

  }

  protected AtomicLong readLastLogId() {
    String filePath = storagePath + File.separator + OPLOG_FILE.replace("$NUM$", "" + info.currentFileNum);
    File f = new File(filePath);
    if (!f.exists()) {
      return new AtomicLong(-1);
    }
    try (RandomAccessFile file = new RandomAccessFile(filePath, "r");) {
      if (file.length() == 0) {
        return new AtomicLong(-1);
      }
      file.seek(file.length() - 16); //length plus magic
      long size = file.readLong();
      long magic = file.readLong();
      if (magic != MAGIC) {
        throw new IllegalStateException();
      }
      file.seek(file.length() - 16 - size - 12);
      return new AtomicLong(file.readLong());
    } catch (IOException e) {
      throw new ODistributedException("Cannot open oplog file: " + e.getMessage());
    }
  }

  @Override
  public OLogId log(ONodeRequest request) {
    return new OLogId(inc.incrementAndGet());
  }

  @Override
  public void logReceived(OLogId logId, ONodeRequest request) {
    write(logId, request);
  }

  private void write(OLogId logId, ONodeRequest request) {
    if (logId.getId() % LOG_ENTRIES_PER_FILE == 0) {
      createNewStreamFile();
    }
    writeRecord(stream, logId, request);
  }

  protected void writeRecord(DataOutputStream stream, OLogId logId, ONodeRequest request) {
    ByteArrayOutputStream outArray = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outArray);
    try {
      request.serialize(out);
      byte[] packet = outArray.toByteArray();
      int packetLengthPlusPacket = packet.length + 4;

      stream.writeLong(logId.getId());
      stream.writeInt(packetLengthPlusPacket);
      stream.writeInt(request.getRequestType());
      stream.write(packet);
      stream.writeLong(packetLengthPlusPacket);
      stream.writeLong(MAGIC);
      stream.flush();
    } catch (IOException e) {
      throw new ODistributedException("Cannot write oplog: " + e.getMessage());
    }
  }

  protected OOperationLogEntry readRecord(DataInputStream stream) {
    try {
      long logId = stream.readLong();
      int totalPacketSize = stream.readInt();
      int packetType = stream.readInt();
      ONodeRequest request = getCoordinateMessagesFactory().createOperationRequest(packetType);
      request.deserialize(stream);
      stream.readLong();//length, again
      long magic = stream.readLong();
      if (magic != MAGIC) {
//        throw //TODO
      }
      return new OOperationLogEntry(new OLogId(logId), request);
    } catch (Exception e) {
      return null;//TODO manage broken log
    }
  }

  protected OCoordinateMessagesFactory getCoordinateMessagesFactory() {
    if (this.factory == null) {
      this.factory = new OCoordinateMessagesFactory();//TODO
    }
    return factory;
  }

  private void createNewStreamFile() {
    info.currentFileNum++;
    File infoFile = new File(storagePath, OPLOG_INFO_FILE);
    writeInfo(infoFile, info);
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    stream = initStream(info);
  }

  @Override
  public Iterator<OOperationLogEntry> iterate(OLogId from, OLogId to) {
    return new OPersistentOperationalLogIterator(this, from, to);
  }

  public void close() {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
