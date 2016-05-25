/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.backup.log;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 30/03/16.
 */
public class OBackupDiskLogger implements OBackupLogger {

  private String           configFile = "${ORIENTDB_HOME}/log/backups_log.txt";

  private File             file;
  private RandomAccessFile randomAccessFile;
  FileChannel              channel;
  OBackupLogFactory        factory;

  public OBackupDiskLogger() {
    initLogger();
    factory = new OBackupLogFactory();

  }

  private void initLogger() {
    file = new File(OSystemVariableResolver.resolveSystemVariables(configFile));

    try {
      if (!file.exists()) {
        file.createNewFile();
      }
      randomAccessFile = new RandomAccessFile(file, "rw");
      channel = randomAccessFile.getChannel();
      channel.position(randomAccessFile.length());
    } catch (IOException e) {
    }

  }

  @Override
  public void log(OBackupLog log) {

    ODocument document = log.toDoc();
    String s = document.toJSON("keepTypes");
    try {
      if (randomAccessFile.length() > 0) {
        s = System.getProperty("line.separator") + s;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    ByteBuffer wrap = ByteBuffer.wrap(s.getBytes());
    try {
      channel.write(wrap);
      channel.force(false);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Override
  public long nextOpId() {
    return System.currentTimeMillis();
  }

  @Override
  public OBackupLog findLast(OBackupLogType op, String uuid) throws IOException {

    OReverseLoggerIterator iterator = new OReverseLoggerIterator(randomAccessFile);
    OBackupLog log = null;
    while (true) {
      String s = iterator.readLine();
      if (s == null || s.isEmpty())
        break;

      ODocument doc = new ODocument().fromJSON(s);
      OBackupLog oBackupLog = factory.fromDoc(doc);
      if (oBackupLog.getType().equals(op) && oBackupLog.getUuid().equals(uuid)) {
        log = oBackupLog;
        break;
      }
    }
    return log;
  }

  @Override
  public OBackupLog findLast(OBackupLogType op, String uuid, Long unitId) throws IOException {
    OReverseLoggerIterator iterator = new OReverseLoggerIterator(randomAccessFile);
    OBackupLog log = null;
    while (true) {
      String s = iterator.readLine();
      if (s == null || s.isEmpty())
        break;

      ODocument doc = new ODocument().fromJSON(s);
      OBackupLog oBackupLog = factory.fromDoc(doc);
      if (oBackupLog.getType().equals(op) && oBackupLog.getUuid().equals(uuid) && unitId.equals(oBackupLog.getUnitId())) {
        log = oBackupLog;
        break;
      }
    }
    return log;
  }

  @Override
  public List<OBackupLog> findByUUID(String uuid, int page, int pageSize, Map<String, String> params) throws IOException {

    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    OReverseLoggerIterator iterator = new OReverseLoggerIterator(randomAccessFile);

    int unitSize = 0;
    while (true) {
      String s = iterator.readLine();
      if (s == null || unitSize == pageSize)
        break;

      ODocument doc = new ODocument().fromJSON(s);
      OBackupLog log = factory.fromDoc(doc);
      if (log.getType().equals(OBackupLogType.BACKUP_SCHEDULED)) {
        unitSize++;
      }
      logs.add(log);
    }
    return logs;
  }

  @Override
  public List<OBackupLog> findByUUIDAndUnitId(String uuid, Long unitId, int page, int pageSize, Map<String, String> params)
      throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    OReverseLoggerIterator iterator = new OReverseLoggerIterator(randomAccessFile);

    int unitSize = 0;
    while (true) {
      String s = iterator.readLine();
      if (s == null || unitSize == pageSize)
        break;

      ODocument doc = new ODocument().fromJSON(s);
      OBackupLog log = factory.fromDoc(doc);
      if (log.getUnitId() == unitId) {
        unitSize++;
        logs.add(log);
      }
    }
    return logs;
  }

  @Override
  public void deleteByUUIDAndUnitIdAndTimestamp(String uuid, Long unitId, Long timestamp) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteByUUIDAndTimestamp(String uuid, Long timestamp) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OBackupLog> findAllLatestByUUID(String uuid, int page, int pageSize) throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    OReverseLoggerIterator iterator = new OReverseLoggerIterator(randomAccessFile);

    Map<Long, Boolean> units = new HashMap<Long, Boolean>();

    int unitSize = 0;
    while (true) {
      String s = iterator.readLine();
      if (s == null || unitSize == pageSize)
        break;
      ODocument doc = new ODocument().fromJSON(s);
      OBackupLog log = factory.fromDoc(doc);
      Boolean aBoolean = units.get(log.getUnitId());
      if (aBoolean == null) {
        unitSize++;
        logs.add(log);
        units.put(log.getUnitId(), true);
      }
    }
    return logs;
  }

  protected class OReverseLoggerIterator {

    private static final int      BUFFER_SIZE   = 8192;
    private final FileChannel     channel;
    private long                  filePos;
    private ByteBuffer            buf;
    private int                   bufPos;
    private byte                  lastLineBreak = '\n';
    private ByteArrayOutputStream baos          = new ByteArrayOutputStream();

    public OReverseLoggerIterator(RandomAccessFile randomAccessFile) throws IOException {
      channel = randomAccessFile.getChannel();
      filePos = randomAccessFile.length();
    }

    public String readLine() throws IOException {

      while (true) {
        if (bufPos < 0) {
          if (filePos == 0) {
            if (baos == null) {
              return null;
            }
            String line = bufToString();
            baos = null;
            return line;
          }

          long start = Math.max(filePos - BUFFER_SIZE, 0);
          long end = filePos;
          long len = end - start;

          buf = channel.map(FileChannel.MapMode.READ_ONLY, start, len);
          bufPos = (int) len;
          filePos = start;
        }

        while (bufPos-- > 0) {
          byte c = buf.get(bufPos);
          if (c == '\r' || c == '\n') {
            if (c != lastLineBreak) {
              lastLineBreak = c;
              continue;
            }
            lastLineBreak = c;
            return bufToString();
          }
          baos.write(c);
        }
      }

    }

    private String bufToString() throws UnsupportedEncodingException {
      if (baos.size() == 0) {
        return "";
      }

      byte[] bytes = baos.toByteArray();
      for (int i = 0; i < bytes.length / 2; i++) {
        byte t = bytes[i];
        bytes[i] = bytes[bytes.length - i - 1];
        bytes[bytes.length - i - 1] = t;
      }

      baos.reset();
      return new String(bytes);
    }
  }
}
