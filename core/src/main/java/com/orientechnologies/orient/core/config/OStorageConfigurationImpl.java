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
package com.orientechnologies.orient.core.config;

import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategyFactory;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OImmutableRecordId;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * <li>14 = no changes, but version was incremented</li>
 * <li>15 = introduced encryption and encryptionKey</li>
 * <li>18 = we keep version of product release under which storage was created</li>
 * <li>19 = Page size and related parameters are stored inside configuration</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("serial")
public class OStorageConfigurationImpl implements OSerializableStream, OStorageConfiguration {
  private static final ORecordId CONFIG_RID = new OImmutableRecordId(0, 0);

  protected final OReadersWriterSpinLock lock = new OReadersWriterSpinLock();

  private                 String                                 charset;
  private final           List<OStorageEntryConfiguration>       properties = new ArrayList<>();
  private final transient OAbstractPaginatedStorage              storage;
  private                 OContextConfiguration                  configuration;
  private                 int                                    version;
  private                 String                                 name;
  private                 String                                 schemaRecordId;
  private                 String                                 dictionaryRecordId;
  private                 String                                 indexMgrRecordId;
  private                 String                                 dateFormat;
  private                 String                                 dateTimeFormat;
  private                 int                                    binaryFormatVersion;
  private                 OStorageSegmentConfiguration           fileTemplate;
  private                 List<OStorageClusterConfiguration>     clusters;
  private                 String                                 localeLanguage;
  private                 String                                 localeCountry;
  private                 TimeZone                               timeZone;
  private transient       Locale                                 localeInstance;
  private                 String                                 clusterSelection;
  private                 String                                 conflictStrategy;
  private                 String                                 recordSerializer;
  private                 int                                    recordSerializerVersion;
  private                 ConcurrentMap<String, IndexEngineData> indexEngines;

  /*
   * Page size and parameters which depend on page size.
   * Such as minimum size of page in free list and maximum size of key in b-tree.
   */
  private int pageSize         = -1;
  private int freeListBoundary = -1;
  private int maxKeySize       = -1;

  private transient boolean                             validation = true;
  protected         OStorageConfigurationUpdateListener updateListener;
  /**
   * Version of product release under which storage was created
   */
  private           String                              createdAtVersion;

  protected final Charset streamCharset;

  public OStorageConfigurationImpl(final OAbstractPaginatedStorage iStorage, Charset streamCharset) {
    lock.acquireWriteLock();
    try {
      this.streamCharset = streamCharset;

      storage = iStorage;

      initConfiguration(new OContextConfiguration());
      clear();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public int getPageSize() {
    lock.acquireReadLock();
    try {
      return pageSize;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setPageSize(int pageSize) {
    lock.acquireWriteLock();
    try {
      this.pageSize = pageSize;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public int getFreeListBoundary() {
    lock.acquireReadLock();
    try {
      return freeListBoundary;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setFreeListBoundary(int freeListBoundary) {
    lock.acquireWriteLock();
    try {
      this.freeListBoundary = freeListBoundary;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public int getMaxKeySize() {
    lock.acquireReadLock();
    try {
      return maxKeySize;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setMaxKeySize(int maxKeySize) {
    lock.acquireWriteLock();
    try {
      this.maxKeySize = maxKeySize;
    } finally {
      lock.releaseWriteLock();
    }
  }

  /**
   * Sets version of product release under which storage was created.
   */
  public void setCreationVersion(String version) {
    lock.acquireWriteLock();
    try {
      this.createdAtVersion = version;
    } finally {
      lock.releaseWriteLock();
    }
  }

  /**
   * @return version of product release under which storage was created.
   */
  public String getCreatedAtVersion() {
    lock.acquireReadLock();
    try {
      return createdAtVersion;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void initConfiguration(OContextConfiguration conf) {
    lock.acquireWriteLock();
    try {
      this.configuration = conf;
    } finally {
      lock.releaseWriteLock();
    }
  }

  private void clear() {
    fileTemplate = new OStorageSegmentConfiguration();

    charset = DEFAULT_CHARSET;
    synchronized (properties) {
      properties.clear();
    }

    version = -1;
    name = null;
    schemaRecordId = null;
    dictionaryRecordId = null;
    indexMgrRecordId = null;
    dateFormat = DEFAULT_DATE_FORMAT;
    dateTimeFormat = DEFAULT_DATETIME_FORMAT;
    binaryFormatVersion = 0;
    clusters = Collections.synchronizedList(new ArrayList<>());
    localeLanguage = Locale.getDefault().getLanguage();
    localeCountry = Locale.getDefault().getCountry();
    timeZone = TimeZone.getDefault();
    localeInstance = null;
    clusterSelection = null;
    conflictStrategy = null;

    getContextConfiguration().setValue(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS,
        OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.getValueAsInteger()); // 0 = AUTOMATIC

    autoInitClusters();

    recordSerializer = null;
    recordSerializerVersion = 0;
    indexEngines = new ConcurrentHashMap<>();
    validation = getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.DB_VALIDATION);

    binaryFormatVersion = CURRENT_BINARY_FORMAT_VERSION;
  }

  private void autoInitClusters() {
    if (getContextConfiguration().getValueAsInteger(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS) == 0) {
      final int cpus = Runtime.getRuntime().availableProcessors();
      getContextConfiguration().setValue(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, cpus > 64 ? 64 : cpus);
    }
  }

  public String getConflictStrategy() {
    lock.acquireReadLock();
    try {
      return conflictStrategy;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setConflictStrategy(String conflictStrategy) {
    lock.acquireWriteLock();
    try {
      this.conflictStrategy = conflictStrategy;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public OContextConfiguration getContextConfiguration() {
    lock.acquireReadLock();
    try {
      return configuration;
    } finally {
      lock.releaseReadLock();
    }
  }

  /**
   * This method load the record information by the internal cluster segment. It's for compatibility with older database than
   * 0.9.25.
   */
  public OStorageConfigurationImpl load(final OContextConfiguration configuration) throws OSerializationException {
    lock.acquireWriteLock();
    try {
      initConfiguration(configuration);

      final byte[] record = storage.readRecord(CONFIG_RID, null, false, false, null).getResult().buffer;

      if (record == null)
        throw new OStorageException("Cannot load database configuration. The database seems corrupted");

      fromStream(record, 0, record.length, streamCharset);
    } finally {
      lock.releaseWriteLock();
    }

    return this;
  }

  public void update() throws OSerializationException {
    lock.acquireWriteLock();
    try {
      final byte[] record = toStream(streamCharset);
      storage.updateRecord(CONFIG_RID, true, record, -1, OBlob.RECORD_TYPE, 0, null);
      if (updateListener != null) {
        updateListener.onUpdate(this);
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getDirectory() {
    lock.acquireReadLock();
    try {
      if (fileTemplate.location != null) {
        return fileTemplate.getLocation();
      }
    } finally {
      lock.releaseReadLock();
    }

    if (storage instanceof OLocalPaginatedStorage) {
      return ((OLocalPaginatedStorage) storage).getStoragePath().toString();
    } else {
      return null;
    }
  }

  public Locale getLocaleInstance() {
    lock.acquireReadLock();
    try {
      if (localeInstance == null) {
        try {
          localeInstance = new Locale(localeLanguage, localeCountry);
        } catch (RuntimeException e) {
          localeInstance = Locale.getDefault();
          OLogManager.instance()
              .errorNoDb(this, "Error during initialization of locale, default one %s will be used", e, localeInstance);

        }
      }

      return localeInstance;
    } finally {
      lock.releaseReadLock();
    }
  }

  public SimpleDateFormat getDateFormatInstance() {
    lock.acquireReadLock();
    try {
      final SimpleDateFormat dateFormatInstance = new SimpleDateFormat(dateFormat);
      dateFormatInstance.setLenient(false);
      dateFormatInstance.setTimeZone(timeZone);

      return dateFormatInstance;
    } finally {
      lock.releaseReadLock();
    }
  }

  public SimpleDateFormat getDateTimeFormatInstance() {
    lock.acquireReadLock();
    try {
      final SimpleDateFormat dateTimeFormatInstance = new SimpleDateFormat(dateTimeFormat);
      dateTimeFormatInstance.setLenient(false);
      dateTimeFormatInstance.setTimeZone(timeZone);
      return dateTimeFormatInstance;
    } finally {
      lock.releaseReadLock();
    }
  }

  @SuppressWarnings("ConstantConditions")
  public void fromStream(final byte[] stream, int offset, int length, Charset charset) {
    lock.acquireWriteLock();
    try {
      clear();

      final String[] values = new String(stream, offset, length, charset).split("\\|");
      int index = 0;
      version = Integer.parseInt(read(values[index++]));

      if (version < 14)
        throw new OStorageException("cannot open database created with a version before 2.0 ");

      name = read(values[index++]);

      schemaRecordId = read(values[index++]);
      dictionaryRecordId = read(values[index++]);

      indexMgrRecordId = read(values[index++]);

      localeLanguage = read(values[index++]);
      localeCountry = read(values[index++]);

      //@COMPATIBILITY with 2.1 version, in this version locale was not mandatory
      if (localeLanguage == null || localeCountry == null) {
        final Locale locale = Locale.getDefault();

        if (localeLanguage == null)
          OLogManager.instance().warn(this,
              "Information about storage locale is undefined (language is undefined) default locale " + locale + " will be used");

        if (localeCountry == null)
          OLogManager.instance().warn(this,
              "Information about storage locale is undefined (country is undefined) default locale " + locale + " will be used");
      }

      dateFormat = read(values[index++]);
      dateTimeFormat = read(values[index++]);

      timeZone = TimeZone.getTimeZone(read(values[index++]));
      this.charset = read(values[index++]);

      final ORecordConflictStrategyFactory conflictStrategyFactory = Orient.instance().getRecordConflictStrategy();
      conflictStrategy = conflictStrategyFactory.getStrategy(read(values[index++])).getName();

      // @COMPATIBILITY
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
        //noinspection ResultOfMethodCallIgnored
        read(values[index++]);

        final String clusterType = read(values[index++]);

        final OStorageClusterConfiguration currentCluster;

        switch (clusterType) {
        case "d":
          final boolean cc = Boolean.valueOf(read(values[index++]));
          final float bb = Float.valueOf(read(values[index++]));
          final float aa = Float.valueOf(read(values[index++]));

          final String clusterCompression = read(values[index++]);

          if (determineStorageCompression == null)
            // TRY TO DETERMINE THE STORAGE COMPRESSION. BEFORE VERSION 11 IT WASN'T STORED IN STORAGE CFG, SO GET FROM THE FIRST
            // CLUSTER
            determineStorageCompression = clusterCompression;

          String clusterEncryption = null;
          if (version >= 15) {
            clusterEncryption = read(values[index++]);
          }

          final String clusterConflictStrategy = read(values[index++]);

          OStorageClusterConfiguration.STATUS status;
          status = OStorageClusterConfiguration.STATUS.valueOf(read(values[index++]));

          final int clusterBinaryVersion;
          if (version >= 21) {
            clusterBinaryVersion = Integer.parseInt(read(values[index++]));
          } else {
            clusterBinaryVersion = 0;
          }

          currentCluster = new OStoragePaginatedClusterConfiguration(this, clusterId, clusterName, null, cc, bb, aa,
              clusterCompression, clusterEncryption, configuration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY),
              clusterConflictStrategy, status, clusterBinaryVersion);

          break;
        case "p":
          // PHYSICAL CLUSTER
          throw new IllegalArgumentException("Cluster of storage 'local' are not supported since 2.0");
        default:
          throw new IllegalArgumentException("Unsupported cluster type: " + clusterType);
        }

        // MAKE ROOMS, EVENTUALLY FILLING EMPTIES ENTRIES
        for (int c = clusters.size(); c <= clusterId; ++c)
          clusters.add(null);

        clusters.set(clusterId, currentCluster);
      }

      size = Integer.parseInt(read(values[index++]));
      clearProperties();
      for (int i = 0; i < size; ++i)
        setProperty(read(values[index++]), read(values[index++]));

      binaryFormatVersion = Integer.parseInt(read(values[index++]));

      clusterSelection = read(values[index++]);

      setMinimumClusters(Integer.parseInt(read(values[index++])));

      autoInitClusters();

      recordSerializer = read(values[index++]);
      recordSerializerVersion = Integer.parseInt(read(values[index++]));

      // READ THE CONFIGURATION
      final int cfgSize = Integer.parseInt(read(values[index++]));
      for (int i = 0; i < cfgSize; ++i) {
        final String key = read(values[index++]);
        final Object value = read(values[index++]);

        final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
        if (cfg != null) {
          if (value != null)
            configuration.setValue(key, OType.convert(value, cfg.getType()));
        } else
          OLogManager.instance().warn(this, "Ignored storage configuration because not supported: %s=%s", key, value);
      }

      if (version > 15) {
        final int enginesSize = Integer.parseInt(read(values[index++]));

        for (int i = 0; i < enginesSize; i++) {
          final String name = read(values[index++]);
          final String algorithm = read(values[index++]);
          final String indexType;

          if (version > 16)
            indexType = read(values[index++]);
          else
            indexType = "";

          final byte valueSerializerId = Byte.parseByte(read(values[index++]));
          final byte keySerializerId = Byte.parseByte(read(values[index++]));

          final boolean isAutomatic = Boolean.parseBoolean(read((values[index++])));
          final Boolean durableInNonTxMode;

          if (read(values[index]) == null) {
            durableInNonTxMode = null;
            index++;
          } else
            durableInNonTxMode = Boolean.parseBoolean(read(values[index++]));

          final int version = Integer.parseInt(read(values[index++]));
          final boolean nullValuesSupport = Boolean.parseBoolean(read((values[index++])));
          final int keySize = Integer.parseInt(read(values[index++]));

          String encryption = null;
          String encryptionOptions = null;
          if (this.version > 19) {
            encryption = read(values[index++]);
            encryptionOptions = read(values[index++]);
          }

          final int typesLength = Integer.parseInt(read(values[index++]));
          final OType[] types = new OType[typesLength];

          for (int n = 0; n < types.length; n++) {
            final OType type = OType.valueOf(read(values[index++]));
            types[n] = type;
          }

          final int propertiesSize = Integer.parseInt(read(values[index++]));
          final Map<String, String> engineProperties;
          if (propertiesSize == 0)
            engineProperties = null;
          else {
            engineProperties = new HashMap<>(propertiesSize);
            for (int n = 0; n < propertiesSize; n++) {
              final String key = read(values[index++]);
              final String value = read(values[index++]);
              engineProperties.put(key, value);
            }
          }

          final IndexEngineData indexEngineData = new IndexEngineData(name, algorithm, indexType, durableInNonTxMode, version,
              valueSerializerId, keySerializerId, isAutomatic, types, nullValuesSupport, keySize, encryption, encryptionOptions,
              engineProperties);

          indexEngines.put(name, indexEngineData);
        }
      }

      if (version > 17) {
        createdAtVersion = read(values[index++]);
      }

      if (version > 18) {
        pageSize = Integer.parseInt(read(values[index++]));
        freeListBoundary = Integer.parseInt(read(values[index++]));
        //noinspection UnusedAssignment
        maxKeySize = Integer.parseInt(read(values[index++]));
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  /**
   * @deprecated because method uses native encoding use {@link #fromStream(byte[], int, int, Charset)} instead.
   */
  @Override
  @Deprecated
  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    fromStream(iStream, 0, iStream.length, Charset.defaultCharset());
    return this;
  }

  /**
   * @deprecated because method uses native encoding use {@link #toStream(Charset)} instead.
   */
  @Override
  public byte[] toStream() throws OSerializationException {
    return toStream(Integer.MAX_VALUE, Charset.defaultCharset());
  }

  public byte[] toStream(Charset charset) throws OSerializationException {
    return toStream(Integer.MAX_VALUE, charset);
  }

  /**
   * Added version used for managed Network Versioning.
   */
  public byte[] toStream(final int iNetworkVersion, Charset charset) throws OSerializationException {
    lock.acquireReadLock();
    try {
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
      if (iNetworkVersion > 24)
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

          if (iNetworkVersion >= 31)
            write(buffer, paginatedClusterConfiguration.encryption);
          if (iNetworkVersion > 24)
            write(buffer, paginatedClusterConfiguration.conflictStrategy);
          if (iNetworkVersion > 25)
            write(buffer, paginatedClusterConfiguration.getStatus().name());

          if (iNetworkVersion >= Integer.MAX_VALUE) {
            write(buffer, paginatedClusterConfiguration.getBinaryVersion());
          }
        }
      }
      if (iNetworkVersion <= 25) {
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
      synchronized (properties) {
        write(buffer, properties.size());
        for (OStorageEntryConfiguration e : properties)
          entryToStream(buffer, e);
      }

      write(buffer, binaryFormatVersion);
      write(buffer, clusterSelection);
      write(buffer, getMinimumClusters());

      if (iNetworkVersion > 24) {
        write(buffer, recordSerializer);
        write(buffer, recordSerializerVersion);

        // WRITE CONFIGURATION
        write(buffer, configuration.getContextSize());
        for (String k : configuration.getContextKeys()) {
          final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(k);
          write(buffer, k);
          if (cfg != null) {
            write(buffer, cfg.isHidden() ? null : configuration.getValueAsString(cfg));
          } else {
            write(buffer, null);
            OLogManager.instance().warn(this, "Storing configuration for property:'" + k + "' not existing in current version");
          }
        }
      }

      write(buffer, indexEngines.size());
      for (IndexEngineData engineData : indexEngines.values()) {
        write(buffer, engineData.name);
        write(buffer, engineData.algorithm);
        write(buffer, engineData.indexType == null ? "" : engineData.indexType);

        write(buffer, engineData.valueSerializerId);
        write(buffer, engineData.keySerializedId);

        write(buffer, engineData.isAutomatic);
        write(buffer, engineData.durableInNonTxMode);

        write(buffer, engineData.version);
        write(buffer, engineData.nullValuesSupport);
        write(buffer, engineData.keySize);
        write(buffer, engineData.encryption);
        write(buffer, engineData.encryptionOptions);

        if (engineData.keyTypes != null) {
          write(buffer, engineData.keyTypes.length);
          for (OType type : engineData.keyTypes) {
            write(buffer, type.name());
          }
        } else {
          write(buffer, 0);
        }

        if (engineData.engineProperties == null) {
          write(buffer, 0);
        } else {
          write(buffer, engineData.engineProperties.size());
          for (Map.Entry<String, String> property : engineData.engineProperties.entrySet()) {
            write(buffer, property.getKey());
            write(buffer, property.getValue());
          }
        }
      }

      write(buffer, createdAtVersion);
      write(buffer, pageSize);
      write(buffer, freeListBoundary);
      write(buffer, maxKeySize);

      // PLAIN: ALLOCATE ENOUGH SPACE TO REUSE IT EVERY TIME
      buffer.append("|");

      return buffer.toString().getBytes(charset);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void create() throws IOException {
    lock.acquireWriteLock();
    try {
      storage.createRecord(CONFIG_RID, new byte[] { 0, 0, 0, 0 }, 0, OBlob.RECORD_TYPE, (byte) 0, null);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void delete() throws IOException {
    lock.acquireWriteLock();
    try {
      close();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void close() throws IOException {
    lock.acquireWriteLock();
    try {
      clear();
      initConfiguration(new OContextConfiguration());
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void dropCluster(final int iClusterId) {
    lock.acquireWriteLock();
    try {
      if (iClusterId < clusters.size()) {
        clusters.set(iClusterId, null);
        update();
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void addCluster(OStoragePaginatedClusterConfiguration config) {
    lock.acquireWriteLock();
    try {
      if (clusters.size() <= config.id) {
        final int diff = config.id - clusters.size();
        for (int i = 0; i < diff; i++) {
          clusters.add(null);
        }

        clusters.add(config);
      } else {
        if (clusters.get(config.id) != null) {
          throw new OStorageException(
              "Cluster with id " + config.id + " already exists with name '" + clusters.get(config.id).getName() + "'");
        }

        clusters.set(config.id, config);
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void addIndexEngine(String name, IndexEngineData engineData) {
    lock.acquireWriteLock();
    try {
      final IndexEngineData oldEngine = indexEngines.putIfAbsent(name, engineData);

      if (oldEngine != null)
        OLogManager.instance()
            .warn(this, "Index engine with name '" + engineData.name + "' already contained in database configuration");

      update();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void deleteIndexEngine(String name) {
    lock.acquireWriteLock();
    try {
      indexEngines.remove(name);
      update();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public Set<String> indexEngines() {
    lock.acquireReadLock();
    try {
      return Collections.unmodifiableSet(indexEngines.keySet());
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public IndexEngineData getIndexEngine(String name) {
    lock.acquireReadLock();
    try {
      return indexEngines.get(name);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setClusterStatus(final int clusterId, final OStorageClusterConfiguration.STATUS iStatus) {
    lock.acquireWriteLock();
    try {
      final OStorageClusterConfiguration clusterCfg = clusters.get(clusterId);
      if (clusterCfg != null)
        clusterCfg.setStatus(iStatus);
      update();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public TimeZone getTimeZone() {
    lock.acquireReadLock();
    try {
      return timeZone;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setTimeZone(final TimeZone timeZone) {
    lock.acquireWriteLock();
    try {
      this.timeZone = timeZone;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getLocaleLanguage() {
    lock.acquireReadLock();
    try {
      return localeLanguage;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setLocaleLanguage(final String iValue) {
    lock.acquireWriteLock();
    try {
      localeLanguage = iValue;
      localeInstance = null;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getLocaleCountry() {
    lock.acquireReadLock();
    try {
      return localeCountry;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setLocaleCountry(final String iValue) {
    lock.acquireWriteLock();
    try {
      localeCountry = iValue;
      localeInstance = null;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getCharset() {
    lock.acquireReadLock();
    try {
      return charset;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setCharset(String charset) {
    lock.acquireWriteLock();
    try {
      this.charset = charset;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getDateFormat() {
    lock.acquireReadLock();
    try {
      return dateFormat;
    } finally {
      lock.releaseReadLock();
    }
  }

  public String getDateTimeFormat() {
    lock.acquireReadLock();
    try {
      return dateTimeFormat;
    } finally {
      lock.releaseReadLock();
    }
  }

  public String getClusterSelection() {
    lock.acquireReadLock();
    try {
      return clusterSelection;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setClusterSelection(final String clusterSelection) {
    lock.acquireWriteLock();
    try {
      this.clusterSelection = clusterSelection;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public int getMinimumClusters() {
    lock.acquireReadLock();
    try {
      final int mc = getContextConfiguration().getValueAsInteger(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS);
      if (mc == 0) {
        autoInitClusters();
        return (Integer) getContextConfiguration().getValue(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS);
      }
      return mc;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setMinimumClusters(final int minimumClusters) {
    lock.acquireWriteLock();
    try {
      getContextConfiguration().setValue(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, minimumClusters);
      autoInitClusters();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getRecordSerializer() {
    lock.acquireReadLock();
    try {
      return recordSerializer;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setRecordSerializer(String recordSerializer) {
    lock.acquireWriteLock();
    try {
      this.recordSerializer = recordSerializer;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public int getRecordSerializerVersion() {
    lock.acquireReadLock();
    try {
      return recordSerializerVersion;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setRecordSerializerVersion(int recordSerializerVersion) {
    lock.acquireWriteLock();
    try {
      this.recordSerializerVersion = recordSerializerVersion;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public boolean isStrictSql() {
    return true;
  }

  public List<OStorageEntryConfiguration> getProperties() {
    lock.acquireReadLock();
    try {
      return Collections.unmodifiableList(properties);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setProperty(final String iName, final String iValue) {
    lock.acquireWriteLock();
    try {

      if ("validation".equalsIgnoreCase(iName))
        validation = "true".equalsIgnoreCase(iValue);

      for (final OStorageEntryConfiguration e : properties) {
        if (e.name.equalsIgnoreCase(iName)) {
          // FOUND: OVERWRITE IT
          e.value = iValue;
          return;
        }
      }

      // NOT FOUND: CREATE IT
      properties.add(new OStorageEntryConfiguration(iName, iValue));
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getProperty(final String iName) {
    lock.acquireReadLock();
    try {
      for (final OStorageEntryConfiguration e : properties) {
        if (e.name.equalsIgnoreCase(iName))
          return e.value;
      }
      return null;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void removeProperty(final String iName) {
    lock.acquireWriteLock();
    try {
      for (Iterator<OStorageEntryConfiguration> it = properties.iterator(); it.hasNext(); ) {
        final OStorageEntryConfiguration e = it.next();
        if (e.name.equalsIgnoreCase(iName)) {
          it.remove();
          break;
        }
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void clearProperties() {
    lock.acquireWriteLock();
    try {
      properties.clear();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public boolean isValidationEnabled() {
    lock.acquireReadLock();
    try {
      return validation;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setValidation(final boolean validation) {
    setProperty("validation", validation ? "true" : "false");
  }

  private int phySegmentFromStream(final String[] values, int index, final OStorageSegmentConfiguration iSegment) {
    iSegment.location = version > 2 ? read(values[index++]) : null;
    iSegment.maxSize = read(values[index++]);
    iSegment.fileType = read(values[index++]);
    iSegment.fileStartSize = read(values[index++]);
    iSegment.fileMaxSize = read(values[index++]);
    iSegment.fileIncrementSize = read(values[index++]);
    iSegment.defrag = read(values[index++]);

    @SuppressWarnings("ConstantConditions")
    final int size = Integer.parseInt(read(values[index++]));
    iSegment.infoFiles = new OStorageFileConfiguration[size];
    String fileName;
    for (int i = 0; i < size; ++i) {
      fileName = read(values[index++]);

      //noinspection ConstantConditions
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

  public static final class IndexEngineData {
    private final String              name;
    private final String              algorithm;
    private final String              indexType;
    private final Boolean             durableInNonTxMode;
    private final int                 version;
    private final byte                valueSerializerId;
    private final byte                keySerializedId;
    private final boolean             isAutomatic;
    private final OType[]             keyTypes;
    private final boolean             nullValuesSupport;
    private final int                 keySize;
    private final Map<String, String> engineProperties;
    private final String              encryption;
    private final String              encryptionOptions;

    public IndexEngineData(final String name, final String algorithm, String indexType, final Boolean durableInNonTxMode,
        final int version, final byte valueSerializerId, final byte keySerializedId, final boolean isAutomatic,
        final OType[] keyTypes, final boolean nullValuesSupport, final int keySize, final String encryption,
        final String encryptionOptions, final Map<String, String> engineProperties) {
      this.name = name;
      this.algorithm = algorithm;
      this.indexType = indexType;
      this.durableInNonTxMode = durableInNonTxMode;
      this.version = version;
      this.valueSerializerId = valueSerializerId;
      this.keySerializedId = keySerializedId;
      this.isAutomatic = isAutomatic;
      this.keyTypes = keyTypes;
      this.nullValuesSupport = nullValuesSupport;
      this.keySize = keySize;
      this.encryption = encryption;
      this.encryptionOptions = encryptionOptions;
      if (engineProperties == null)
        this.engineProperties = null;
      else
        this.engineProperties = new HashMap<>(engineProperties);
    }

    public int getKeySize() {
      return keySize;
    }

    public String getName() {
      return name;
    }

    public String getAlgorithm() {
      return algorithm;
    }

    public Boolean getDurableInNonTxMode() {
      return durableInNonTxMode;
    }

    public int getVersion() {
      return version;
    }

    public byte getValueSerializerId() {
      return valueSerializerId;
    }

    public byte getKeySerializedId() {
      return keySerializedId;
    }

    public boolean isAutomatic() {
      return isAutomatic;
    }

    public OType[] getKeyTypes() {
      return keyTypes;
    }

    public String getEncryption() {
      return encryption;
    }

    public String getEncryptionOptions() {
      return encryptionOptions;
    }

    public boolean isNullValuesSupport() {
      return nullValuesSupport;
    }

    public Map<String, String> getEngineProperties() {
      if (engineProperties == null)
        return null;

      return Collections.unmodifiableMap(engineProperties);
    }

    public String getIndexType() {
      return indexType;
    }
  }

  @Override
  public String getSchemaRecordId() {
    lock.acquireReadLock();
    try {
      return schemaRecordId;
    } finally {
      lock.releaseReadLock();
    }

  }

  public void setSchemaRecordId(String schemaRecordId) {
    lock.acquireWriteLock();
    try {
      this.schemaRecordId = schemaRecordId;
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getIndexMgrRecordId() {
    lock.acquireReadLock();
    try {
      return indexMgrRecordId;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setIndexMgrRecordId(String indexMgrRecordId) {
    lock.acquireWriteLock();
    try {
      this.indexMgrRecordId = indexMgrRecordId;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void setDateFormat(String dateFormat) {
    lock.acquireWriteLock();
    try {
      this.dateFormat = dateFormat;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    lock.acquireWriteLock();
    try {
      this.dateTimeFormat = dateTimeFormat;
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public int getBinaryFormatVersion() {
    lock.acquireReadLock();
    try {
      return binaryFormatVersion;
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public String getName() {
    lock.acquireReadLock();
    try {
      return name;
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public int getVersion() {
    lock.acquireReadLock();
    try {
      return version;
    } finally {
      lock.releaseReadLock();
    }
  }

  public List<OStorageClusterConfiguration> getClusters() {
    lock.acquireReadLock();
    try {
      return Collections.unmodifiableList(clusters);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setConfigurationUpdateListener(OStorageConfigurationUpdateListener updateListener) {
    lock.acquireWriteLock();
    try {
      this.updateListener = updateListener;
    } finally {
      lock.releaseWriteLock();
    }
  }
}
