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
 * </ul>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class OStorageConfiguration implements OSerializableStream {
  public static final ORecordId                           CONFIG_RID                    = new OImmutableRecordId(0, 0);

  public static final String                              DEFAULT_CHARSET               = "UTF-8";
  private String                                          charset;
  public static final int                                 CURRENT_VERSION               = 16;
  public static final int                                 CURRENT_BINARY_FORMAT_VERSION = 12;
  private volatile List<OStorageEntryConfiguration>       properties;
  protected final transient OStorage                      storage;
  private volatile OContextConfiguration                  configuration;
  public volatile int                                     version;
  public volatile String                                  name;
  public volatile String                                  schemaRecordId;
  public volatile String                                  dictionaryRecordId;
  public volatile String                                  indexMgrRecordId;
  public volatile String                                  dateFormat;
  public volatile String                                  dateTimeFormat;
  public volatile int                                     binaryFormatVersion;
  public volatile OStorageSegmentConfiguration            fileTemplate;
  public volatile List<OStorageClusterConfiguration>      clusters;
  private volatile String                                 localeLanguage;
  private volatile String                                 localeCountry;
  private volatile TimeZone                               timeZone;
  private transient volatile Locale                       localeInstance;
  private transient volatile DecimalFormatSymbols         unusualSymbols;
  private volatile String                                 clusterSelection;
  private volatile String                                 conflictStrategy;
  private volatile int                                    minimumClusters;
  private volatile String                                 recordSerializer;
  private volatile int                                    recordSerializerVersion;
  private volatile boolean                                strictSQL;
  private volatile Map<String, Object>                    loadProperties;
  private volatile ConcurrentMap<String, IndexEngineData> indexEngines;
  private volatile transient boolean                      validation                    = true;

  public OStorageConfiguration(final OStorage iStorage) {
    storage = iStorage;

    clear();

    initConfiguration();
  }

  public void initConfiguration() {
    configuration = new OContextConfiguration();
  }

  public void clear() {
    fileTemplate = new OStorageSegmentConfiguration();

    charset = DEFAULT_CHARSET;
    properties = Collections.synchronizedList(new ArrayList<OStorageEntryConfiguration>());

    version = -1;
    name = null;
    schemaRecordId = null;
    dictionaryRecordId = null;
    indexMgrRecordId = null;
    dateFormat = "yyyy-MM-dd";
    dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    binaryFormatVersion = 0;
    clusters = Collections.synchronizedList(new ArrayList<OStorageClusterConfiguration>());
    localeLanguage = Locale.getDefault().getLanguage();
    localeCountry = Locale.getDefault().getCountry();
    timeZone = TimeZone.getDefault();
    localeInstance = null;
    unusualSymbols = null;
    clusterSelection = null;
    conflictStrategy = null;
    minimumClusters = 1;
    recordSerializer = null;
    recordSerializerVersion = 0;
    strictSQL = false;
    indexEngines = new ConcurrentHashMap<String, IndexEngineData>();
    validation = OGlobalConfiguration.DB_VALIDATION.getValueAsBoolean();

    binaryFormatVersion = CURRENT_BINARY_FORMAT_VERSION;
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
   * @param iProperties
   */
  public OStorageConfiguration load(final Map<String, Object> iProperties) throws OSerializationException {
    initConfiguration();

    final String compressionMethod = (String) iProperties.get(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getKey()
        .toLowerCase());
    if (compressionMethod != null)
      // SAVE COMPRESSION METHOD IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, compressionMethod);

    final String encryptionMethod = (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey().toLowerCase());
    if (encryptionMethod != null)
      // SAVE ENCRYPTION METHOD IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, encryptionMethod);

    final String encryptionKey = (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey().toLowerCase());
    if (encryptionKey != null)
      // SAVE ENCRYPTION KEY IN CONFIGURATION
      configuration.setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, encryptionKey);

    final byte[] record = storage.readRecord(CONFIG_RID, null, false, null).getResult().buffer;

    if (record == null)
      throw new OStorageException("Cannot load database configuration. The database seems to be corrupted");

    fromStream(record);

    this.loadProperties = new HashMap<String, Object>(iProperties);

    return this;
  }

  public Map<String, Object> getLoadProperties() {
    if (loadProperties == null)
      return Collections.emptyMap();

    return Collections.unmodifiableMap(loadProperties);
  }

  public void update() throws OSerializationException {
    final byte[] record = toStream();
    storage.updateRecord(CONFIG_RID, true, record, OVersionFactory.instance().createUntrackedVersion(), ORecordBytes.RECORD_TYPE,
        0, null);
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

  public void fromStream(final byte[] stream, int offset, int length) {
    clear();

    final String[] values = new String(stream, offset, length).split("\\|");
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

        String clusterEncryption = null;
        if (version >= 15)
          clusterEncryption = read(values[index++]);

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
            clusterCompression, clusterEncryption, configuration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY),
            clusterConflictStrategy, status);

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
        if (cfg != null) {
          if (value != null)
            configuration.setValue(key, OType.convert(value, cfg.getType()));
        } else
          OLogManager.instance().warn(this, "Ignored storage configuration because not supported: %s=%s", key, value);
      }
    } else
      // SAVE STORAGE COMPRESSION METHOD AS PROPERTY
      configuration.setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, determineStorageCompression);

    if (version > 15) {
      final int enginesSize = Integer.parseInt(read(values[index++]));

      for (int i = 0; i < enginesSize; i++) {
        final String name = read(values[index++]);
        final String algorithm = read(values[index++]);

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
          engineProperties = new HashMap<String, String>(propertiesSize);
          for (int n = 0; n < propertiesSize; n++) {
            final String key = read(values[index++]);
            final String value = read(values[index++]);
            engineProperties.put(key, value);
          }
        }

        final IndexEngineData indexEngineData = new IndexEngineData(name, algorithm, durableInNonTxMode, version,
            valueSerializerId, keySerializerId, isAutomatic, types, nullValuesSupport, keySize, engineProperties);

        indexEngines.put(name.toLowerCase(getLocaleInstance()), indexEngineData);
      }
    }

  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    fromStream(iStream, 0, iStream.length);
    return this;
  }

  public byte[] toStream() throws OSerializationException {
    return toStream(Integer.MAX_VALUE);
  }

  /**
   * Added version used for managed Network Versioning.
   * 
   * @param iNetworkVersion
   * @return
   * @throws OSerializationException
   */
  public byte[] toStream(final int iNetworkVersion) throws OSerializationException {
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
          write(buffer, paginatedClusterConfiguration.getStatus().name().toString());
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
    write(buffer, properties.size());
    for (OStorageEntryConfiguration e : properties)
      entryToStream(buffer, e);

    write(buffer, binaryFormatVersion);
    write(buffer, clusterSelection);
    write(buffer, minimumClusters);

    if (iNetworkVersion > 24) {
      write(buffer, recordSerializer);
      write(buffer, recordSerializerVersion);

      // WRITE CONFIGURATION
      write(buffer, configuration.getContextSize());
      for (String k : configuration.getContextKeys()) {
        final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(k);

        write(buffer, k);
        write(buffer, cfg.isHidden() ? null : configuration.getValueAsString(cfg));
      }
    }

    write(buffer, indexEngines.size());
    for (IndexEngineData engineData : indexEngines.values()) {
      write(buffer, engineData.name);
      write(buffer, engineData.algorithm);

      write(buffer, engineData.valueSerializerId);
      write(buffer, engineData.keySerializedId);

      write(buffer, engineData.isAutomatic);
      write(buffer, engineData.durableInNonTxMode);

      write(buffer, engineData.version);
      write(buffer, engineData.nullValuesSupport);
      write(buffer, engineData.keySize);

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
    clear();

    initConfiguration();
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

  public void addIndexEngine(String name, IndexEngineData engineData) {
    if (indexEngines.containsKey(name))
      throw new IllegalArgumentException("Index engine with name " + engineData.name
          + " already contained in database configuration");

    final IndexEngineData oldEngine = indexEngines.putIfAbsent(name, engineData);

    if (oldEngine != null)
      throw new IllegalArgumentException("Index engine with name " + engineData.name
          + " already contained in database configuration");

    update();
  }

  public void deleteIndexEngine(String name) {
    indexEngines.remove(name);
    update();
  }

  public Set<String> indexEngines() {
    return Collections.unmodifiableSet(indexEngines.keySet());
  }

  public IndexEngineData getIndexEngine(String name) {
    return indexEngines.get(name);
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

  public List<OStorageEntryConfiguration> getProperties() {
    return Collections.unmodifiableList(properties);
  }

  public void setProperty(final String iName, final String iValue) {
    if (OStatement.CUSTOM_STRICT_SQL.equalsIgnoreCase(iName))
      // SET STRICT SQL VARIABLE
      strictSQL = "true".equalsIgnoreCase("" + iValue);

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

  public boolean isValidationEnabled() {
    return validation;
  }

  public void setValidation(final boolean validation) {
    this.validation = validation;
  }

  protected void bindPropertiesToContext(final Map<String, Object> iProperties) {
    final String compressionMethod = iProperties != null ? (String) iProperties.get(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD
        .getKey().toLowerCase()) : null;
    if (compressionMethod != null)
      // SAVE COMPRESSION METHOD IN CONFIGURATION
      getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, compressionMethod);

    final String encryptionMethod = iProperties != null ? (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD
        .getKey().toLowerCase()) : null;
    if (encryptionMethod != null)
      // SAVE ENCRYPTION METHOD IN CONFIGURATION
      getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, encryptionMethod);

    final String encryptionKey = iProperties != null ? (String) iProperties.get(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY
        .getKey().toLowerCase()) : null;
    if (encryptionKey != null)
      // SAVE ENCRYPTION KEY IN CONFIGURATION
      getContextConfiguration().setValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, encryptionKey);
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

  public static final class IndexEngineData {
    private final String              name;
    private final String              algorithm;
    private final Boolean             durableInNonTxMode;
    private final int                 version;
    private final byte                valueSerializerId;
    private final byte                keySerializedId;
    private final boolean             isAutomatic;
    private final OType[]             keyTypes;
    private final boolean             nullValuesSupport;
    private final int                 keySize;
    private final Map<String, String> engineProperties;

    public IndexEngineData(String name, String algorithm, Boolean durableInNonTxMode, int version, byte valueSerializerId,
        byte keySerializedId, boolean isAutomatic, OType[] keyTypes, boolean nullValuesSupport, int keySize,
        Map<String, String> engineProperties) {
      this.name = name;
      this.algorithm = algorithm;
      this.durableInNonTxMode = durableInNonTxMode;
      this.version = version;
      this.valueSerializerId = valueSerializerId;
      this.keySerializedId = keySerializedId;
      this.isAutomatic = isAutomatic;
      this.keyTypes = keyTypes;
      this.nullValuesSupport = nullValuesSupport;
      this.keySize = keySize;
      if (engineProperties == null)
        this.engineProperties = null;
      else
        this.engineProperties = new HashMap<String, String>(engineProperties);
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

    public boolean isNullValuesSupport() {
      return nullValuesSupport;
    }

    public Map<String, String> getEngineProperties() {
      if (engineProperties == null)
        return null;

      return Collections.unmodifiableMap(engineProperties);
    }
  }
}
