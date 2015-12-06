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
package com.orientechnologies.orient.core.config;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategyFactory;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OImmutableRecordId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.ORoundRobinClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.io.IOException;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Versions:
 * <ul>
 * <li>3 = introduced file directory in physical segments and data-segment id in clusters</li>
 * <li>4 = ??</li>
 * <li>5 = ??</li>
 * <li>6 = ??</li>
 * <li>7 = ??</li>
 * <li>8 = introduced cluster selection strategy as string</li>
 * <li>9 = introduced minimumclusters as string</li>
 * <li>12 = introduced record conflict strategy as string in both storage and paginated clusters</li>
 * <li>13 = introduced cluster status to manage cluster as "offline" with the new command "alter cluster status offline". Removed
 * data segments</li>
 * </ul>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class OStorageConfiguration implements OSerializableStream {
  public static final ORecordId CONFIG_RID = new OImmutableRecordId(0, 0);

  public static final String                         DEFAULT_CHARSET               = "UTF-8";
  private String                                     charset                       = DEFAULT_CHARSET;
  public static final int                            CURRENT_VERSION               = 14;
  public static final int                            CURRENT_BINARY_FORMAT_VERSION = 12;
  private final List<OStorageEntryConfiguration>     properties                    = Collections
      .synchronizedList(new ArrayList<OStorageEntryConfiguration>());
  protected final transient OStorage                 storage;
  private final OContextConfiguration                configuration                 = new OContextConfiguration();
  public volatile int                                version                       = -1;
  public volatile String                             name;
  public volatile String                             schemaRecordId;
  public volatile String                             dictionaryRecordId;
  public volatile String                             indexMgrRecordId;
  public volatile String                             dateFormat                    = "yyyy-MM-dd";
  public volatile String                             dateTimeFormat                = "yyyy-MM-dd HH:mm:ss";
  public volatile int                                binaryFormatVersion;
  public volatile OStorageSegmentConfiguration       fileTemplate;
  public volatile List<OStorageClusterConfiguration> clusters                      = Collections
      .synchronizedList(new ArrayList<OStorageClusterConfiguration>());
  private volatile String                            localeLanguage                = Locale.getDefault().getLanguage();
  private volatile String                            localeCountry                 = Locale.getDefault().getCountry();
  private volatile TimeZone                          timeZone                      = TimeZone.getDefault();
  private transient volatile Locale                  localeInstance;
  private transient volatile DecimalFormatSymbols    unusualSymbols;
  private volatile String                            clusterSelection;
  private volatile String                            conflictStrategy;
  private volatile int                               minimumClusters               = 1;
  private volatile String                            recordSerializer;
  private volatile int                               recordSerializerVersion;
  private volatile boolean                           strictSQL;
  private volatile boolean                           txRequiredForSQLGraphOperations;

  public OStorageConfiguration(final OStorage iStorage) {
    storage = iStorage;
    fileTemplate = new OStorageSegmentConfiguration();

    binaryFormatVersion = CURRENT_BINARY_FORMAT_VERSION;

    txRequiredForSQLGraphOperations = OGlobalConfiguration.SQL_GRAPH_CONSISTENCY_MODE.getValueAsString().equalsIgnoreCase("tx");
  }

  public String getConflictStrategy() {
    return conflictStrategy;
  }

  public void setConflictStrategy(String conflictStrategy) {
    this.conflictStrategy = conflictStrategy;
  }

  public OContextConfiguration getContextConfiguration() {
    return configuration;
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
    final byte[] record = storage.readRecord(CONFIG_RID, null, false, null).getResult().buffer;

    if (record == null)
      throw new OStorageException("Cannot load database's configuration. The database seems corrupted");

    fromStream(record);
    return this;
  }

  public void update() throws OSerializationException {
    final byte[] record = toStream();
    storage.updateRecord(CONFIG_RID, true, record, OVersionFactory.instance().createUntrackedVersion(), ORecordBytes.RECORD_TYPE, 0,
        null);
  }

  public boolean isEmpty() {
    return clusters.isEmpty();
  }

  public String getDirectory() {
    return fileTemplate.location != null ? fileTemplate.getLocation() : ((OLocalPaginatedStorage) storage).getStoragePath();
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
      // @COMPATIBILITY
      indexMgrRecordId = null;

    localeLanguage = read(values[index++]);
    localeCountry = read(values[index++]);
    dateFormat = read(values[index++]);
    dateTimeFormat = read(values[index++]);

    // @COMPATIBILITY 1.2.0
    if (version >= 4) {
      timeZone = TimeZone.getTimeZone(read(values[index++]));
      charset = read(values[index++]);
    }

    final ORecordConflictStrategyFactory conflictStrategyFactory = Orient.instance().getRecordConflictStrategy();
    if (version >= 12)
      conflictStrategy = conflictStrategyFactory.getStrategy(read(values[index++])).getName();
    else
      conflictStrategy = conflictStrategyFactory.getDefaultStrategy();

    // @COMPATIBILTY
    if (version > 1)
      index = phySegmentFromStream(values, index, fileTemplate);

    int size = Integer.parseInt(read(values[index++]));

    // PREPARE THE LIST OF CLUSTERS
    clusters.clear();

    String determineStorageCompression = null;

    for (int i = 0; i < size; ++i) {
      final int clusterId = Integer.parseInt(read(values[index++]));

      if (clusterId == -1)
        continue;

      final String clusterName = read(values[index++]);
      final int targetDataSegmentId = version >= 3 ? Integer.parseInt(read(values[index++])) : 0;

      final String clusterType = read(values[index++]);

      final OStorageClusterConfiguration currentCluster;

      if (clusterType.equals("d")) {
        final boolean cc = Boolean.valueOf(read(values[index++]));
        final float bb = Float.valueOf(read(values[index++]));
        final float aa = Float.valueOf(read(values[index++]));
        final String clusterCompression = read(values[index++]);

        if (determineStorageCompression == null)
          // TRY TO DETERMINE THE STORAGE COMPRESSION. BEFORE VERSION 11 IT WASN'T STORED IN STORAGE CFG, SO GET FROM THE FIRST
          // CLUSTER
          determineStorageCompression = clusterCompression;

        final String clusterConflictStrategy;
        if (version >= 12)
          clusterConflictStrategy = read(values[index++]);
        else
          // INHERIT THE STRATEGY IN STORAGE
          clusterConflictStrategy = null;

        OStorageClusterConfiguration.STATUS status = OStorageClusterConfiguration.STATUS.ONLINE;
        if (version >= 13)
          status = OStorageClusterConfiguration.STATUS.valueOf(read(values[index++]));

        currentCluster = new OStoragePaginatedClusterConfiguration(this, clusterId, clusterName, null, cc, bb, aa,
            clusterCompression, clusterConflictStrategy, status);

      } else if (clusterType.equals("p"))
        // PHYSICAL CLUSTER
        throw new IllegalArgumentException("Cluster of storage 'local' are not supported since 2.0");
      else
        throw new IllegalArgumentException("Unsupported cluster type: " + clusterType);

      // MAKE ROOMS, EVENTUALLY FILLING EMPTIES ENTRIES
      for (int c = clusters.size(); c <= clusterId; ++c)
        clusters.add(null);

      clusters.set(clusterId, currentCluster);
    }

    if (version < 13) {
      // OLD: READ DATA-SEGMENTS
      size = Integer.parseInt(read(values[index++]));

      for (int i = 0; i < size; ++i) {
        int dataId = Integer.parseInt(read(values[index++]));
        if (dataId == -1)
          continue;
        read(values[index++]);
        read(values[index++]);
        read(values[index++]);
        read(values[index++]);
      }

      // READ TX_SEGMENT STUFF
      read(values[index++]);
      read(values[index++]);
      read(values[index++]);
      read(values[index++]);
      read(values[index++]);
    }

    size = Integer.parseInt(read(values[index++]));
    clearProperties();
    for (int i = 0; i < size; ++i)
      setProperty(read(values[index++]), read(values[index++]));

    if (version >= 7)
      binaryFormatVersion = Integer.parseInt(read(values[index++]));
    else if (version == 6)
      binaryFormatVersion = 9;
    else
      binaryFormatVersion = 8;

    if (version >= 8)
      clusterSelection = read(values[index++]);
    else
      // DEFAULT = ROUND-ROBIN
      clusterSelection = ORoundRobinClusterSelectionStrategy.NAME;

    if (version >= 9)
      minimumClusters = Integer.parseInt(read(values[index++]));
    else
      // DEFAULT = 1
      minimumClusters = 1;

    if (version >= 10) {
      recordSerializer = read(values[index++]);
      recordSerializerVersion = Integer.parseInt(read(values[index++]));
    }

    if (version >= 11) {
      // READ THE CONFIGURATION
      final int cfgSize = Integer.parseInt(read(values[index++]));
      for (int i = 0; i < cfgSize; ++i) {
        final String key = read(values[index++]);
        final Object value = read(values[index++]);

        final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
        if (cfg != null)
          configuration.setValue(key, OType.convert(value, cfg.getType()));
        else
          OLogManager.instance().warn(this, "Ignored storage configuration because not supported: %s=%s.", key, value);
      }
    } else
      // SAVE STORAGE COMPRESSION METHOD AS PROPERTY
      configuration.setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, determineStorageCompression);

    return this;
  }

  public byte[] toStream() throws OSerializationException {
    return toStream(Integer.MAX_VALUE);
  }

  /**
   * Added version used for managed Network Versioning.
   * 
   * @param version
   * @return
   * @throws OSerializationException
   */
  public byte[] toStream(int version) throws OSerializationException {
    final StringBuilder buffer = new StringBuilder(8192);

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
    if (version > 24)
      write(buffer, conflictStrategy);

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

      if (c instanceof OStoragePaginatedClusterConfiguration) {
        write(buffer, "d");

        final OStoragePaginatedClusterConfiguration paginatedClusterConfiguration = (OStoragePaginatedClusterConfiguration) c;

        write(buffer, paginatedClusterConfiguration.useWal);
        write(buffer, paginatedClusterConfiguration.recordOverflowGrowFactor);
        write(buffer, paginatedClusterConfiguration.recordGrowFactor);
        write(buffer, paginatedClusterConfiguration.compression);
        if (version > 24)
          write(buffer, paginatedClusterConfiguration.conflictStrategy);
        if (version > 25)
          write(buffer, paginatedClusterConfiguration.getStatus().name().toString());
      }
    }
    if (version <= 25) {
      // dataSegment array
      write(buffer, 0);
      // tx Segment File
      write(buffer, "");
      write(buffer, "");
      write(buffer, 0);
      // tx segment flags
      write(buffer, false);
      write(buffer, false);
    }
    write(buffer, properties.size());
    for (OStorageEntryConfiguration e : properties)
      entryToStream(buffer, e);

    write(buffer, binaryFormatVersion);
    write(buffer, clusterSelection);
    write(buffer, minimumClusters);

    if (version > 24) {
      write(buffer, recordSerializer);
      write(buffer, recordSerializerVersion);

      // WRITE CONFIGURATION
      write(buffer, configuration.getContextSize());
      for (String k : configuration.getContextKeys()) {
        write(buffer, k);
        write(buffer, configuration.getValueAsString(OGlobalConfiguration.findByKey(k)));
      }
    }

    // PLAIN: ALLOCATE ENOUGH SPACE TO REUSE IT EVERY TIME
    buffer.append("|");

    return buffer.toString().getBytes();
  }

  public void lock() throws IOException {
  }

  public void unlock() throws IOException {
  }

  public void create() throws IOException {
    storage.createRecord(CONFIG_RID, new byte[] { 0, 0, 0, 0 }, OVersionFactory.instance().createVersion(),
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

  public void setClusterStatus(final int clusterId, final OStorageClusterConfiguration.STATUS iStatus) {
    final OStorageClusterConfiguration clusterCfg = clusters.get(clusterId);
    if (clusterCfg != null)
      clusterCfg.setStatus(iStatus);
    update();
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

  public void setLocaleLanguage(final String iValue) {
    localeLanguage = iValue;
    localeInstance = null;
  }

  public String getLocaleCountry() {
    return localeCountry;
  }

  public void setLocaleCountry(final String iValue) {
    localeCountry = iValue;
    localeInstance = null;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public String getClusterSelection() {
    return clusterSelection;
  }

  public void setClusterSelection(final String clusterSelection) {
    this.clusterSelection = clusterSelection;
  }

  public int getMinimumClusters() {
    return minimumClusters;
  }

  public void setMinimumClusters(final int minimumClusters) {
    this.minimumClusters = minimumClusters;
  }

  public String getRecordSerializer() {
    return recordSerializer;
  }

  public void setRecordSerializer(String recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  public int getRecordSerializerVersion() {
    return recordSerializerVersion;
  }

  public void setRecordSerializerVersion(int recordSerializerVersion) {
    this.recordSerializerVersion = recordSerializerVersion;
  }

  public boolean isStrictSql() {
    return strictSQL;
  }

  public boolean isTxRequiredForSQLGraphOperations() {
    return txRequiredForSQLGraphOperations;
  }

  public List<OStorageEntryConfiguration> getProperties() {
    return Collections.unmodifiableList(properties);
  }

  public void setProperty(final String iName, final String iValue) {
    if (OStatement.CUSTOM_STRICT_SQL.equalsIgnoreCase(iName))
      // SET STRICT SQL VARIABLE
      strictSQL = "true".equalsIgnoreCase("" + iValue);

    if ("txRequiredForSQLGraphOperations".equalsIgnoreCase(iName))
      // SET TX SQL GRAPH OPERATIONS
      txRequiredForSQLGraphOperations = "true".equalsIgnoreCase(iValue);

    for (Iterator<OStorageEntryConfiguration> it = properties.iterator(); it.hasNext();) {
      final OStorageEntryConfiguration e = it.next();
      if (e.name.equalsIgnoreCase(iName)) {
        // FOUND: OVERWRITE IT
        e.value = iValue;
        return;
      }
    }

    // NOT FOUND: CREATE IT
    properties.add(new OStorageEntryConfiguration(iName, iValue));
  }

  public String getProperty(final String iName) {
    for (Iterator<OStorageEntryConfiguration> it = properties.iterator(); it.hasNext();) {
      final OStorageEntryConfiguration e = it.next();
      if (e.name.equalsIgnoreCase(iName))
        return e.value;
    }
    return null;
  }

  public boolean existsProperty(final String iName) {
    for (Iterator<OStorageEntryConfiguration> it = properties.iterator(); it.hasNext();) {
      final OStorageEntryConfiguration e = it.next();
      if (e.name.equalsIgnoreCase(iName))
        return true;
    }
    return false;
  }

  public void removeProperty(final String iName) {
    for (Iterator<OStorageEntryConfiguration> it = properties.iterator(); it.hasNext();) {
      final OStorageEntryConfiguration e = it.next();
      if (e.name.equalsIgnoreCase(iName)) {
        it.remove();
        break;
      }
    }
  }

  public void clearProperties() {
    properties.clear();
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
}
