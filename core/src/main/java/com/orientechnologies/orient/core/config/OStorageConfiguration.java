/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.config;

import java.io.IOException;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.OImmutableRecordId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * Versions:<li>
 * <ul>
 * 3 = introduced file directory in physical segments and data-segment id in clusters
 * </ul>
 * </li>
 * 
 * @author Luca
 * 
 */
@SuppressWarnings("serial")
public class OStorageConfiguration implements OSerializableStream {
  public static final ORecordId             CONFIG_RID      = new OImmutableRecordId(0, OClusterPositionFactory.INSTANCE.valueOf(0));

  public static final String                DEFAULT_CHARSET = "UTF-8";

  public static final int                   CURRENT_VERSION = 6;

  public int                                version         = -1;
  public String                             name;
  public String                             schemaRecordId;
  public String                             dictionaryRecordId;
  public String                             indexMgrRecordId;

  private String                            localeLanguage  = Locale.getDefault().getLanguage();
  private String                            localeCountry   = Locale.getDefault().getCountry();
  public String                             dateFormat      = "yyyy-MM-dd";
  public String                             dateTimeFormat  = "yyyy-MM-dd HH:mm:ss";
  private TimeZone                          timeZone        = TimeZone.getDefault();
  private String                            charset         = DEFAULT_CHARSET;

  public OStorageSegmentConfiguration       fileTemplate;

  public List<OStorageClusterConfiguration> clusters        = new ArrayList<OStorageClusterConfiguration>();
  public List<OStorageDataConfiguration>    dataSegments    = new ArrayList<OStorageDataConfiguration>();

  public OStorageTxConfiguration            txSegment       = new OStorageTxConfiguration();

  public List<OStorageEntryConfiguration>   properties      = new ArrayList<OStorageEntryConfiguration>();

  private transient Locale                  localeInstance;
  private transient DecimalFormatSymbols    unusualSymbols;
  protected transient OStorage              storage;

  public OStorageConfiguration(final OStorage iStorage) {
    storage = iStorage;
    fileTemplate = new OStorageSegmentConfiguration();
  }

  /**
   * This method load the record information by the internal cluster segment. It's for compatibility with older database than
   * 0.9.25.
   * 
   * @compatibility 0.9.25
   * @return
   * @throws OSerializationException
   */
  public OStorageConfiguration load() throws OSerializationException {
    final byte[] record = storage.readRecord(CONFIG_RID, null, false, null, false).getResult().buffer;

    if (record == null)
      throw new OStorageException("Cannot load database's configuration. The database seems to be corrupted.");

    fromStream(record);
    return this;
  }

  public void update() throws OSerializationException {
    final byte[] record = toStream();
    storage
        .updateRecord(CONFIG_RID, record, OVersionFactory.instance().createUntrackedVersion(), ORecordBytes.RECORD_TYPE, 0, null);
  }

  public boolean isEmpty() {
    return clusters.isEmpty();
  }

  public String getDirectory() {
    return fileTemplate.location != null ? fileTemplate.getLocation() : ((OStorageLocalAbstract) storage).getStoragePath();
  }

  public Locale getLocaleInstance() {
    if (localeInstance == null)
      localeInstance = new Locale(localeLanguage, localeCountry);

    return localeInstance;
  }

  public void resetLocaleInstance() {
    localeInstance = null;
  }

  public SimpleDateFormat getDateFormatInstance() {
    final SimpleDateFormat dateFormatInstance = new SimpleDateFormat(dateFormat);
    dateFormatInstance.setLenient(false);
    dateFormatInstance.setTimeZone(timeZone);
    return dateFormatInstance;
  }

  public SimpleDateFormat getDateTimeFormatInstance() {
    final SimpleDateFormat dateTimeFormatInstance = new SimpleDateFormat(dateTimeFormat);
    dateTimeFormatInstance.setLenient(false);
    dateTimeFormatInstance.setTimeZone(timeZone);
    return dateTimeFormatInstance;
  }

  public DecimalFormatSymbols getUnusualSymbols() {
    if (unusualSymbols == null)
      unusualSymbols = new DecimalFormatSymbols(getLocaleInstance());
    return unusualSymbols;
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    final String[] values = new String(iStream).split("\\|");
    int index = 0;
    version = Integer.parseInt(read(values[index++]));

    name = read(values[index++]);

    schemaRecordId = read(values[index++]);
    dictionaryRecordId = read(values[index++]);

    if (version > 0)
      indexMgrRecordId = read(values[index++]);
    else
      // @COMPATIBILTY
      indexMgrRecordId = null;

    localeLanguage = read(values[index++]);
    localeCountry = read(values[index++]);
    dateFormat = read(values[index++]);
    dateTimeFormat = read(values[index++]);

    // @COMPATIBILTY 1.2.0
    if (version >= 4) {
      timeZone = TimeZone.getTimeZone(read(values[index++]));
      charset = read(values[index++]);
    }

    // @COMPATIBILTY
    if (version > 1)
      index = phySegmentFromStream(values, index, fileTemplate);

    int size = Integer.parseInt(read(values[index++]));

    // PREPARE THE LIST OF CLUSTERS
    clusters = new ArrayList<OStorageClusterConfiguration>(size);

    for (int i = 0; i < size; ++i) {
      final int clusterId = Integer.parseInt(read(values[index++]));

      if (clusterId == -1)
        continue;

      final String clusterName = read(values[index++]);
      final int targetDataSegmentId = version >= 3 ? Integer.parseInt(read(values[index++])) : 0;

      final String clusterType = read(values[index++]);

      final OStorageClusterConfiguration currentCluster;

      if (clusterType.equals("p")) {
        // PHYSICAL CLUSTER
        final OStoragePhysicalClusterConfigurationLocal phyClusterLocal = new OStoragePhysicalClusterConfigurationLocal(this,
            clusterId, targetDataSegmentId);
        phyClusterLocal.name = clusterName;
        index = phySegmentFromStream(values, index, phyClusterLocal);

        final String holeFlag;
        if (version > 4) {
          holeFlag = read(values[index++]);
        } else {
          holeFlag = "f";
        }
        if (holeFlag.equals("f"))
          phyClusterLocal.setHoleFile(new OStorageClusterHoleConfiguration(phyClusterLocal, read(values[index++]),
              read(values[index++]), read(values[index++])));
        currentCluster = phyClusterLocal;
      } else if (clusterType.equals("m"))
        // MEMORY CLUSTER
        currentCluster = new OStorageMemoryClusterConfiguration(clusterName, clusterId, targetDataSegmentId);
      else if (clusterType.equals("d")) {
        currentCluster = new OStoragePaginatedClusterConfiguration(this, clusterId, clusterName, null,
            Boolean.valueOf(read(values[index++])), Float.valueOf(read(values[index++])), Float.valueOf(read(values[index++])),
            read(values[index++]));
      } else
        throw new IllegalArgumentException("Unsupported cluster type: " + clusterType);

      // MAKE ROOMS, EVENTUALLY FILLING EMPTIES ENTRIES
      for (int c = clusters.size(); c <= clusterId; ++c)
        clusters.add(null);

      clusters.set(clusterId, currentCluster);
    }

    // PREPARE THE LIST OF DATA SEGS
    size = Integer.parseInt(read(values[index++]));
    dataSegments = new ArrayList<OStorageDataConfiguration>(size);
    for (int i = 0; i < size; ++i)
      dataSegments.add(null);

    int dataId;
    String dataName;
    OStorageDataConfiguration data;
    for (int i = 0; i < size; ++i) {
      dataId = Integer.parseInt(read(values[index++]));
      if (dataId == -1)
        continue;
      dataName = read(values[index++]);

      data = new OStorageDataConfiguration(this, dataName, dataId);
      index = phySegmentFromStream(values, index, data);
      data.holeFile = new OStorageDataHoleConfiguration(data, read(values[index++]), read(values[index++]), read(values[index++]));
      dataSegments.set(dataId, data);
    }

    txSegment = new OStorageTxConfiguration(read(values[index++]), read(values[index++]), read(values[index++]),
        read(values[index++]), read(values[index++]));

    size = Integer.parseInt(read(values[index++]));
    properties = new ArrayList<OStorageEntryConfiguration>(size);
    for (int i = 0; i < size; ++i) {
      properties.add(new OStorageEntryConfiguration(read(values[index++]), read(values[index++])));
    }

    return this;
  }

  public byte[] toStream() throws OSerializationException {
    final StringBuilder buffer = new StringBuilder();

    write(buffer, CURRENT_VERSION);
    write(buffer, name);

    write(buffer, schemaRecordId);
    write(buffer, dictionaryRecordId);
    write(buffer, indexMgrRecordId);

    write(buffer, localeLanguage);
    write(buffer, localeCountry);
    write(buffer, dateFormat);
    write(buffer, dateTimeFormat);

    write(buffer, timeZone.getID());
    write(buffer, charset);

    phySegmentToStream(buffer, fileTemplate);

    write(buffer, clusters.size());
    for (OStorageClusterConfiguration c : clusters) {
      if (c == null) {
        write(buffer, -1);
        continue;
      }

      write(buffer, c.getId());
      write(buffer, c.getName());
      write(buffer, c.getDataSegmentId());

      if (c instanceof OStoragePhysicalClusterConfigurationLocal) {
        // PHYSICAL
        write(buffer, "p");
        phySegmentToStream(buffer, (OStoragePhysicalClusterConfigurationLocal) c);

        OStorageFileConfiguration holeFile = ((OStoragePhysicalClusterConfigurationLocal) c).getHoleFile();
        if (holeFile == null)
          write(buffer, "e");
        else
          write(buffer, "f");

        if (holeFile != null)
          fileToStream(buffer, holeFile);

      } else if (c instanceof OStorageMemoryClusterConfiguration) {
        // MEMORY
        write(buffer, "m");
      } else if (c instanceof OStorageEHClusterConfiguration) {
        write(buffer, "h");
      } else if (c instanceof OStoragePaginatedClusterConfiguration) {
        write(buffer, "d");

        final OStoragePaginatedClusterConfiguration paginatedClusterConfiguration = (OStoragePaginatedClusterConfiguration) c;

        write(buffer, paginatedClusterConfiguration.useWal);
        write(buffer, paginatedClusterConfiguration.recordOverflowGrowFactor);
        write(buffer, paginatedClusterConfiguration.recordGrowFactor);
        write(buffer, paginatedClusterConfiguration.compression);
      }
    }

    write(buffer, dataSegments.size());
    for (OStorageDataConfiguration d : dataSegments) {
      if (d == null) {
        write(buffer, -1);
        continue;
      }

      write(buffer, d.id);
      write(buffer, d.name);

      phySegmentToStream(buffer, d);
      fileToStream(buffer, d.holeFile);
    }

    fileToStream(buffer, txSegment);
    write(buffer, txSegment.isSynchRecord());
    write(buffer, txSegment.isSynchTx());

    write(buffer, properties.size());
    for (OStorageEntryConfiguration e : properties)
      entryToStream(buffer, e);

    // PLAIN: ALLOCATE ENOUGHT SPACE TO REUSE IT EVERY TIME
    buffer.append("|");

    return buffer.toString().getBytes();
  }

  private int phySegmentFromStream(final String[] values, int index, final OStorageSegmentConfiguration iSegment) {
    iSegment.location = version > 2 ? read(values[index++]) : null;
    iSegment.maxSize = read(values[index++]);
    iSegment.fileType = read(values[index++]);
    iSegment.fileStartSize = read(values[index++]);
    iSegment.fileMaxSize = read(values[index++]);
    iSegment.fileIncrementSize = read(values[index++]);
    iSegment.defrag = read(values[index++]);

    final int size = Integer.parseInt(read(values[index++]));
    iSegment.infoFiles = new OStorageFileConfiguration[size];
    String fileName;
    for (int i = 0; i < size; ++i) {
      fileName = read(values[index++]);

      if (!fileName.contains("$")) {
        // @COMPATIBILITY 0.9.25
        int pos = fileName.indexOf("/databases");
        if (pos > -1) {
          fileName = "${" + Orient.ORIENTDB_HOME + "}" + fileName.substring(pos);
        }
      }

      iSegment.infoFiles[i] = new OStorageFileConfiguration(iSegment, fileName, read(values[index++]), read(values[index++]),
          iSegment.fileIncrementSize);
    }

    return index;
  }

  private void phySegmentToStream(final StringBuilder iBuffer, final OStorageSegmentConfiguration iSegment) {
    write(iBuffer, iSegment.location);
    write(iBuffer, iSegment.maxSize);
    write(iBuffer, iSegment.fileType);
    write(iBuffer, iSegment.fileStartSize);
    write(iBuffer, iSegment.fileMaxSize);
    write(iBuffer, iSegment.fileIncrementSize);
    write(iBuffer, iSegment.defrag);

    write(iBuffer, iSegment.infoFiles.length);
    for (OStorageFileConfiguration f : iSegment.infoFiles)
      fileToStream(iBuffer, f);
  }

  private void fileToStream(final StringBuilder iBuffer, final OStorageFileConfiguration iFile) {
    write(iBuffer, iFile.path);
    write(iBuffer, iFile.type);
    write(iBuffer, iFile.maxSize);
  }

  private void entryToStream(final StringBuilder iBuffer, final OStorageEntryConfiguration iEntry) {
    write(iBuffer, iEntry.name);
    write(iBuffer, iEntry.value);
  }

  private String read(final String iValue) {
    if (iValue.equals(" "))
      return null;
    return iValue;
  }

  private void write(final StringBuilder iBuffer, final Object iValue) {
    if (iBuffer.length() > 0)
      iBuffer.append('|');
    iBuffer.append(iValue != null ? iValue.toString() : ' ');
  }

  public void create() throws IOException {
    storage.createRecord(0, CONFIG_RID, new byte[] { 0, 0, 0, 0 }, OVersionFactory.instance().createVersion(),
        ORecordBytes.RECORD_TYPE, (byte) 0, null);
  }

  public void synch() throws IOException {
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  public void close() throws IOException {
  }

  public void setCluster(final OStorageClusterConfiguration config) {
    while (config.getId() >= clusters.size())
      clusters.add(null);
    clusters.set(config.getId(), config);
  }

  public void dropCluster(final int iClusterId) {
    if (iClusterId < clusters.size()) {
      clusters.set(iClusterId, null);
      update();
    }
  }

  public void dropDataSegment(final int iId) {
    if (iId < dataSegments.size()) {
      dataSegments.set(iId, null);
      update();
    }
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(final TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public String getLocaleLanguage() {
    return localeLanguage;
  }

  public String getLocaleCountry() {
    return localeCountry;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public void setLocaleLanguage(final String iValue) {
    localeLanguage = iValue;
    localeInstance = null;
  }

  public void setLocaleCountry(final String iValue) {
    localeCountry = iValue;
    localeInstance = null;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }
}
