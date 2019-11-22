package com.orientechnologies.orient.core.storage.config;

import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.*;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPaginatedClusterFactory;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeSingleValueV1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class OClusterBasedStorageConfiguration implements OStorageConfiguration {
  public static final String MAP_FILE_EXTENSION  = ".ccm";
  public static final String DATA_FILE_EXTENSION = ".cd";

  public static final String TREE_DATA_FILE_EXTENSION = ".bd";
  public static final String TREE_NULL_FILE_EXTENSION = ".nd";

  public static final  String COMPONENT_NAME                   = "config";
  private static final String VERSION_PROPERTY                 = "version";
  private static final String SCHEMA_RECORD_ID_PROPERTY        = "schemaRecordId";
  private static final String INDEX_MANAGER_RECORD_ID_PROPERTY = "indexManagerRecordId";

  private static final String LOCALE_LANGUAGE_PROPERTY = "localeLanguage";
  private static final String LOCALE_COUNTRY_PROPERTY  = "localeCountry";
  private static final String LOCALE_PROPERTY_INSTANCE = "localeInstance";

  private static final String DATE_FORMAT_PROPERTY      = "dateFormat";
  private static final String DATE_TIME_FORMAT_PROPERTY = "dateTimeFormat";

  private static final String TIME_ZONE_PROPERTY                 = "timeZone";
  private static final String CHARSET_PROPERTY                   = "charset";
  private static final String CONFLICT_STRATEGY_PROPERTY         = "conflictStrategy";
  private static final String BINARY_FORMAT_VERSION_PROPERTY     = "binaryFormatVersion";
  private static final String CLUSTER_SELECTION_PROPERTY         = "clusterSelection";
  private static final String MINIMUM_CLUSTERS_PROPERTY          = "minimumClusters";
  private static final String RECORD_SERIALIZER_PROPERTY         = "recordSerializer";
  private static final String RECORD_SERIALIZER_VERSION_PROPERTY = "recordSerializerVersion";
  private static final String CONFIGURATION_PROPERTY             = "configuration";
  private static final String CREATED_AT_VERSION_PROPERTY        = "createAtVersion";
  private static final String PAGE_SIZE_PROPERTY                 = "pageSize";
  private static final String FREE_LIST_BOUNDARY_PROPERTY        = "freeListBoundary";
  private static final String MAX_KEY_SIZE_PROPERTY              = "maxKeySize";

  private static final String CLUSTERS_PREFIX_PROPERTY = "cluster_";
  private static final String PROPERTY_PREFIX_PROPERTY = "property_";
  private static final String ENGINE_PREFIX_PROPERTY   = "engine_";

  private static final String PROPERTIES = "properties";
  private static final String CLUSTERS   = "clusters";
  private static final String UUID       = "UUID";

  private static final String[] INT_PROPERTIES = new String[] { MINIMUM_CLUSTERS_PROPERTY, VERSION_PROPERTY,
      BINARY_FORMAT_VERSION_PROPERTY, RECORD_SERIALIZER_VERSION_PROPERTY, PAGE_SIZE_PROPERTY, FREE_LIST_BOUNDARY_PROPERTY,
      MAX_KEY_SIZE_PROPERTY };

  private static final String[] STRING_PROPERTIES = new String[] { SCHEMA_RECORD_ID_PROPERTY, INDEX_MANAGER_RECORD_ID_PROPERTY,
      LOCALE_LANGUAGE_PROPERTY, LOCALE_COUNTRY_PROPERTY, DATE_FORMAT_PROPERTY, DATE_TIME_FORMAT_PROPERTY, TIME_ZONE_PROPERTY,
      CHARSET_PROPERTY, CONFLICT_STRATEGY_PROPERTY, CLUSTER_SELECTION_PROPERTY, RECORD_SERIALIZER_PROPERTY,
      CREATED_AT_VERSION_PROPERTY, UUID };

  private OContextConfiguration configuration;
  private boolean               validation;

  private final CellBTreeSingleValueV1<String> btree;
  private final OPaginatedCluster              cluster;

  private final OAbstractPaginatedStorage storage;
  private final OAtomicOperationsManager  atomicOperationsManager;
  private final OReadersWriterSpinLock    lock = new OReadersWriterSpinLock();

  private final HashMap<String, Object> cache = new HashMap<>();

  private OStorageConfigurationUpdateListener updateListener;

  private final ThreadLocal<PausedNotificationsState> pauseNotifications = ThreadLocal.withInitial(PausedNotificationsState::new);

  public static boolean exists(final OWriteCache writeCache) {
    return writeCache.exists(COMPONENT_NAME + DATA_FILE_EXTENSION);
  }

  public OClusterBasedStorageConfiguration(final OAbstractPaginatedStorage storage) {
    cluster = OPaginatedClusterFactory
        .createCluster(COMPONENT_NAME, OPaginatedCluster.getLatestBinaryVersion(), storage, DATA_FILE_EXTENSION,
            MAP_FILE_EXTENSION);
    btree = new CellBTreeSingleValueV1<>(COMPONENT_NAME, TREE_DATA_FILE_EXTENSION, TREE_NULL_FILE_EXTENSION, storage);
    this.atomicOperationsManager = storage.getAtomicOperationsManager();

    this.storage = storage;
  }

  public void create(final OContextConfiguration contextConfiguration) throws IOException {
    lock.acquireWriteLock();
    try {
      cluster.create();
      btree.create(OStringSerializer.INSTANCE, null, 1, null);

      this.configuration = contextConfiguration;

      init();

      preloadIntProperties();
      preloadStringProperties();
      preloadClusters();
      preloadConfigurationProperties();
      setValidation(getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.DB_VALIDATION));
      recalculateLocale();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void create(final OContextConfiguration contextConfiguration, final OStorageConfiguration source) throws IOException {
    lock.acquireWriteLock();
    try {
      create(contextConfiguration);
      copy(source);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void delete() throws IOException {
    lock.acquireWriteLock();
    try {
      updateListener = null;

      final long firstPosition = cluster.getFirstPosition();
      OPhysicalPosition[] positions = cluster.ceilingPositions(new OPhysicalPosition(firstPosition));
      while (positions.length > 0) {
        for (OPhysicalPosition position : positions) {
          cluster.deleteRecord(position.clusterPosition);
        }

        positions = cluster.higherPositions(positions[positions.length - 1]);
      }

      cluster.delete();

      final Stream<String> keyStream = btree.keyStream();
      keyStream.forEach((key) -> {
        try {
          btree.remove(key);
        } catch (IOException e) {
          throw OException
              .wrapException(new OStorageException("Can not clear index inside of storage " + storage.getName() + " configuration"),
                  e);
        }
      });

      btree.delete();

      cache.clear();
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void close() {
    lock.acquireWriteLock();
    try {
      updateListener = null;

      updateConfigurationProperty();
      updateMinimumClusters();

      cache.clear();

      //tree and cluster will be closed by storage automatically
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void load(final OContextConfiguration configuration) throws OSerializationException, IOException {
    lock.acquireWriteLock();
    try {
      this.configuration = configuration;

      cluster.open();
      btree.load(COMPONENT_NAME, 1, null, OStringSerializer.INSTANCE, null);

      readConfiguration();
      readMinimumClusters();

      preloadIntProperties();
      preloadStringProperties();
      preloadConfigurationProperties();
      preloadClusters();
      recalculateLocale();

      validation = "true".equalsIgnoreCase(getProperty("validation"));
    } finally {
      lock.releaseWriteLock();
    }

  }

  public void pauseUpdateNotifications() {
    lock.acquireWriteLock();
    try {
      final PausedNotificationsState pausedNotificationsState = pauseNotifications.get();
      pausedNotificationsState.notificationsPaused = true;
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void fireUpdateNotifications() {
    lock.acquireWriteLock();
    try {
      final PausedNotificationsState pausedNotificationsState = pauseNotifications.get();

      if (pausedNotificationsState.pendingChanges > 0 && updateListener != null) {
        updateListener.onUpdate(this);
        pausedNotificationsState.pendingChanges = 0;
      }

      pausedNotificationsState.notificationsPaused = false;
    } finally {
      lock.releaseWriteLock();
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

  private void updateMinimumClusters() {
    updateIntProperty(MINIMUM_CLUSTERS_PROPERTY, getMinimumClusters());
  }

  private void readMinimumClusters() {
    if (containsProperty(MINIMUM_CLUSTERS_PROPERTY)) {
      setMinimumClusters(readIntProperty(MINIMUM_CLUSTERS_PROPERTY, false));
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

  @Override
  public OContextConfiguration getContextConfiguration() {
    lock.acquireReadLock();
    try {
      return configuration;
    } finally {
      lock.releaseReadLock();
    }
  }

  /**
   * Added version used for managed Network Versioning.
   */
  public byte[] toStream(final int iNetworkVersion, final Charset charset) throws OSerializationException {
    lock.acquireReadLock();
    try {
      final StringBuilder buffer = new StringBuilder(8192);

      write(buffer, CURRENT_VERSION);
      write(buffer, null);

      write(buffer, getSchemaRecordId());
      write(buffer, "");
      write(buffer, getIndexMgrRecordId());

      write(buffer, getLocaleLanguage());
      write(buffer, getLocaleCountry());
      write(buffer, getDateFormat());
      write(buffer, getDateFormat());

      final TimeZone timeZone = getTimeZone();
      assert timeZone != null;

      write(buffer, timeZone);
      write(buffer, charset);
      if (iNetworkVersion > 24) {
        write(buffer, getConflictStrategy());
      }

      phySegmentToStream(buffer, new OStorageSegmentConfiguration());

      final List<OStorageClusterConfiguration> clusters = getClusters();
      write(buffer, clusters.size());
      for (final OStorageClusterConfiguration c : clusters) {
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

          if (iNetworkVersion >= 31) {
            write(buffer, paginatedClusterConfiguration.encryption);
          }
          if (iNetworkVersion > 24) {
            write(buffer, paginatedClusterConfiguration.conflictStrategy);
          }
          if (iNetworkVersion > 25) {
            write(buffer, paginatedClusterConfiguration.getStatus().name());
          }

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

      final List<OStorageEntryConfiguration> properties = getProperties();
      write(buffer, properties.size());
      for (final OStorageEntryConfiguration e : properties) {
        entryToStream(buffer, e);
      }

      write(buffer, getBinaryFormatVersion());
      write(buffer, getClusterSelection());
      write(buffer, getMinimumClusters());

      if (iNetworkVersion > 24) {
        write(buffer, getRecordSerializer());
        write(buffer, getRecordSerializerVersion());

        // WRITE CONFIGURATION
        write(buffer, configuration.getContextSize());
        for (final String k : configuration.getContextKeys()) {
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

      final List<IndexEngineData> engines = loadIndexEngines();
      write(buffer, engines.size());
      for (final IndexEngineData engineData : engines) {
        write(buffer, engineData.getName());
        write(buffer, engineData.getAlgorithm());
        write(buffer, engineData.getIndexType() == null ? "" : engineData.getIndexType());

        write(buffer, engineData.getValueSerializerId());
        write(buffer, engineData.getKeySerializedId());

        write(buffer, engineData.isAutomatic());
        write(buffer, engineData.getDurableInNonTxMode());

        write(buffer, engineData.getVersion());
        write(buffer, engineData.isNullValuesSupport());
        write(buffer, engineData.getKeySize());
        write(buffer, engineData.getEncryption());
        write(buffer, engineData.getEncryptionOptions());

        if (engineData.getKeyTypes() != null) {
          write(buffer, engineData.getKeyTypes().length);
          for (final OType type : engineData.getKeyTypes()) {
            write(buffer, type.name());
          }
        } else {
          write(buffer, 0);
        }

        if (engineData.getEngineProperties() == null) {
          write(buffer, 0);
        } else {
          write(buffer, engineData.getEngineProperties().size());
          for (final Map.Entry<String, String> property : engineData.getEngineProperties().entrySet()) {
            write(buffer, property.getKey());
            write(buffer, property.getValue());
          }
        }

        write(buffer, engineData.getApiVersion());
        write(buffer, engineData.isMultivalue());
      }

      write(buffer, getCreatedAtVersion());
      write(buffer, getPageSize());
      write(buffer, getFreeListBoundary());
      write(buffer, getMaxKeySize());

      // PLAIN: ALLOCATE ENOUGH SPACE TO REUSE IT EVERY TIME
      buffer.append("|");

      return buffer.toString().getBytes(charset);
    } finally {
      lock.releaseReadLock();
    }
  }

  private static void entryToStream(final StringBuilder buffer, final OStorageEntryConfiguration entry) {
    write(buffer, entry.name);
    write(buffer, entry.value);
  }

  private static void phySegmentToStream(final StringBuilder buffer, final OStorageSegmentConfiguration segment) {
    write(buffer, segment.getLocation());
    write(buffer, segment.maxSize);
    write(buffer, segment.fileType);
    write(buffer, segment.fileStartSize);
    write(buffer, segment.fileMaxSize);
    write(buffer, segment.fileIncrementSize);
    write(buffer, segment.defrag);

    write(buffer, segment.infoFiles.length);
    for (final OStorageFileConfiguration f : segment.infoFiles) {
      fileToStream(buffer, f);
    }
  }

  private static void fileToStream(final StringBuilder iBuffer, final OStorageFileConfiguration iFile) {
    write(iBuffer, iFile.path);
    write(iBuffer, iFile.type);
    write(iBuffer, iFile.maxSize);
  }

  private static void write(final StringBuilder buffer, final Object value) {
    if (buffer.length() > 0) {
      buffer.append('|');
    }

    buffer.append(value != null ? value.toString() : ' ');
  }

  private void updateVersion() {
    updateIntProperty(VERSION_PROPERTY, CURRENT_VERSION);
  }

  private void updateVersion(final int version) {
    updateIntProperty(VERSION_PROPERTY, version);
  }

  @Override
  public int getVersion() {
    lock.acquireReadLock();
    try {
      return readIntProperty(VERSION_PROPERTY, true);
    } finally {
      lock.releaseReadLock();
    }

  }

  @Override
  public String getName() {
    return null;
  }

  public void setSchemaRecordId(final String schemaRecordId) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(SCHEMA_RECORD_ID_PROPERTY, schemaRecordId, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getSchemaRecordId() {
    lock.acquireReadLock();
    try {
      return readStringProperty(SCHEMA_RECORD_ID_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setIndexMgrRecordId(final String indexMgrRecordId) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(INDEX_MANAGER_RECORD_ID_PROPERTY, indexMgrRecordId, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getIndexMgrRecordId() {
    lock.acquireReadLock();
    try {
      return readStringProperty(INDEX_MANAGER_RECORD_ID_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setLocaleLanguage(final String value) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(LOCALE_LANGUAGE_PROPERTY, value, true);

      recalculateLocale();
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getLocaleLanguage() {
    lock.acquireReadLock();
    try {
      return readStringProperty(LOCALE_LANGUAGE_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setLocaleCountry(final String value) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(LOCALE_COUNTRY_PROPERTY, value, true);

      recalculateLocale();
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getLocaleCountry() {
    lock.acquireReadLock();
    try {
      return readStringProperty(LOCALE_COUNTRY_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setDateFormat(final String dateFormat) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(DATE_FORMAT_PROPERTY, dateFormat, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getDateFormat() {
    lock.acquireReadLock();
    try {
      final String dateFormat = readStringProperty(DATE_FORMAT_PROPERTY);
      assert dateFormat != null;

      return dateFormat;
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public SimpleDateFormat getDateFormatInstance() {
    lock.acquireReadLock();
    try {
      final SimpleDateFormat dateFormatInstance = new SimpleDateFormat(getDateFormat());
      dateFormatInstance.setLenient(false);
      final TimeZone timeZone = getTimeZone();
      if (timeZone != null) {
        dateFormatInstance.setTimeZone(timeZone);
      }

      return dateFormatInstance;
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public String getDateTimeFormat() {
    lock.acquireReadLock();
    try {
      final String dateTimeFormat = readStringProperty(DATE_TIME_FORMAT_PROPERTY);
      assert dateTimeFormat != null;

      return dateTimeFormat;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setDateTimeFormat(final String dateTimeFormat) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(DATE_TIME_FORMAT_PROPERTY, dateTimeFormat, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void setUuid(final String uuid) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(UUID, uuid, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public String getUuid() {
    lock.acquireReadLock();
    try {
      final String uuid = readStringProperty(UUID);
      assert uuid != null;

      return uuid;
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public SimpleDateFormat getDateTimeFormatInstance() {
    lock.acquireReadLock();
    try {
      final SimpleDateFormat dateTimeFormatInstance = new SimpleDateFormat(getDateTimeFormat());
      dateTimeFormatInstance.setLenient(false);
      final TimeZone timeZone = getTimeZone();
      if (timeZone != null) {
        dateTimeFormatInstance.setTimeZone(timeZone);
      }

      return dateTimeFormatInstance;
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setTimeZone(final TimeZone timeZone) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(TIME_ZONE_PROPERTY, timeZone.getID(), true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public TimeZone getTimeZone() {
    lock.acquireReadLock();
    try {
      final String timeZone = readStringProperty(TIME_ZONE_PROPERTY);
      if (timeZone == null) {
        return null;
      }

      return TimeZone.getTimeZone(timeZone);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setCharset(final String charset) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(CHARSET_PROPERTY, charset, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getCharset() {
    lock.acquireReadLock();
    try {
      return readStringProperty(CHARSET_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setConflictStrategy(final String conflictStrategy) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(CONFLICT_STRATEGY_PROPERTY, conflictStrategy, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getConflictStrategy() {
    lock.acquireReadLock();
    try {
      return readStringProperty(CONFLICT_STRATEGY_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  private void updateBinaryFormatVersion() {
    updateIntProperty(BINARY_FORMAT_VERSION_PROPERTY, CURRENT_BINARY_FORMAT_VERSION);
  }

  private void updateBinaryFormatVersion(final int version) {
    updateIntProperty(BINARY_FORMAT_VERSION_PROPERTY, version);
  }

  @Override
  public int getBinaryFormatVersion() {
    lock.acquireReadLock();
    try {
      return readIntProperty(BINARY_FORMAT_VERSION_PROPERTY, true);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setClusterSelection(final String clusterSelection) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(CLUSTER_SELECTION_PROPERTY, clusterSelection, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getClusterSelection() {
    lock.acquireReadLock();
    try {
      return readStringProperty(CLUSTER_SELECTION_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setRecordSerializer(final String recordSerializer) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(RECORD_SERIALIZER_PROPERTY, recordSerializer, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getRecordSerializer() {
    lock.acquireReadLock();
    try {
      return readStringProperty(RECORD_SERIALIZER_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setRecordSerializerVersion(final int recordSerializerVersion) {
    lock.acquireWriteLock();
    try {
      updateIntProperty(RECORD_SERIALIZER_VERSION_PROPERTY, recordSerializerVersion);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public int getRecordSerializerVersion() {
    lock.acquireReadLock();
    try {
      return readIntProperty(RECORD_SERIALIZER_VERSION_PROPERTY, true);
    } finally {
      lock.releaseReadLock();
    }
  }

  private void updateConfigurationProperty() {
    final List<byte[]> entries = new ArrayList<>(8);
    int totalSize = 0;

    final byte[] contextSize = new byte[OIntegerSerializer.INT_SIZE];
    totalSize += contextSize.length;
    entries.add(contextSize);

    OIntegerSerializer.INSTANCE.serializeNative(configuration.getContextSize(), contextSize, 0);

    for (final String k : configuration.getContextKeys()) {
      final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(k);
      final byte[] key = serializeStringValue(k);
      totalSize += key.length;
      entries.add(key);

      if (cfg != null) {
        final byte[] value = serializeStringValue(cfg.isHidden() ? null : configuration.getValueAsString(cfg));
        totalSize += value.length;
        entries.add(value);
      } else {
        final byte[] value = serializeStringValue(null);
        totalSize += value.length;
        entries.add(value);

        OLogManager.instance().warn(this, "Storing configuration for property:'" + k + "' not existing in current version");
      }
    }

    final byte[] property = mergeBinaryEntries(totalSize, entries);
    storeProperty(CONFIGURATION_PROPERTY, property);
  }

  private void readConfiguration() {
    final byte[] property = readProperty(CONFIGURATION_PROPERTY);

    if (property == null) {
      return;
    }

    int pos = 0;
    final int size = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < size; i++) {
      final String key = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      final String value = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
      if (cfg != null) {
        if (value != null) {
          configuration.setValue(key, OType.convert(value, cfg.getType()));
        }
      } else {
        OLogManager.instance().warn(this, "Ignored storage configuration because not supported: %s=%s", key, value);
      }
    }
  }

  public void setCreationVersion(final String version) {
    lock.acquireWriteLock();
    try {
      updateStringProperty(CREATED_AT_VERSION_PROPERTY, version, true);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public String getCreatedAtVersion() {
    lock.acquireReadLock();
    try {
      return readStringProperty(CREATED_AT_VERSION_PROPERTY);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setPageSize(final int pageSize) {
    lock.acquireWriteLock();
    try {
      updateIntProperty(PAGE_SIZE_PROPERTY, pageSize);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public int getPageSize() {
    lock.acquireReadLock();
    try {
      return readIntProperty(PAGE_SIZE_PROPERTY, true);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setFreeListBoundary(final int freeListBoundary) {
    lock.acquireWriteLock();
    try {
      updateIntProperty(FREE_LIST_BOUNDARY_PROPERTY, freeListBoundary);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public int getFreeListBoundary() {
    lock.acquireReadLock();
    try {
      return readIntProperty(FREE_LIST_BOUNDARY_PROPERTY, true);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setMaxKeySize(final int maxKeySize) {
    lock.acquireWriteLock();
    try {
      updateIntProperty(MAX_KEY_SIZE_PROPERTY, maxKeySize);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public int getMaxKeySize() {
    lock.acquireReadLock();
    try {
      return readIntProperty(MAX_KEY_SIZE_PROPERTY, true);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void setProperty(final String name, final String value) {
    lock.acquireWriteLock();
    try {
      if ("validation".equalsIgnoreCase(name)) {
        validation = "true".equalsIgnoreCase(value);
      }

      final String key = PROPERTY_PREFIX_PROPERTY + name;
      updateStringProperty(key, value, false);

      @SuppressWarnings("unchecked")
      final Map<String, String> properties = (Map<String, String>) cache.get(PROPERTIES);
      properties.put(name, value);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void setValidation(final boolean validation) {
    setProperty("validation", validation ? "true" : "false");
  }

  @Override
  public boolean isValidationEnabled() {
    lock.acquireReadLock();
    try {
      return validation;
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public String getDirectory() {
    if (storage instanceof OLocalPaginatedStorage) {
      return ((OLocalPaginatedStorage) storage).getStoragePath().toString();
    } else {
      return null;
    }
  }

  @Override
  public String getProperty(final String name) {
    lock.acquireReadLock();
    try {
      @SuppressWarnings("unchecked")
      final Map<String, String> properties = (Map<String, String>) cache.get(PROPERTIES);
      return properties.get(name);
    } finally {
      lock.releaseReadLock();
    }
  }

  @Override
  public List<OStorageEntryConfiguration> getProperties() {
    lock.acquireReadLock();
    try {
      @SuppressWarnings("unchecked")
      final Map<String, String> properties = (Map<String, String>) cache.get(PROPERTIES);

      final List<OStorageEntryConfiguration> result = new ArrayList<>(8);

      for (final Map.Entry<String, String> entry : properties.entrySet()) {
        result.add(new OStorageEntryConfiguration(entry.getKey(), entry.getValue()));
      }

      return result;
    } finally {
      lock.releaseReadLock();
    }
  }

  private void preloadConfigurationProperties() {
    final Stream<ORawPair<String, ORID>> stream = btree.iterateEntriesMajor(PROPERTY_PREFIX_PROPERTY, false, true);
    final Map<String, String> properties = stream.filter((pair) -> pair.first.startsWith(PROPERTY_PREFIX_PROPERTY)).map((entry) -> {
      final ORawBuffer buffer;
      try {
        buffer = cluster.readRecord(entry.second.getClusterPosition(), false);
        return new ORawPair<>(entry.first.substring(PROPERTY_PREFIX_PROPERTY.length()), deserializeStringValue(buffer.buffer, 0));
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException("Can not preload configuration properties"), e);
      }
    }).collect(Collectors.toMap((pair) -> pair.first, (pair) -> pair.second));

    cache.put(PROPERTIES, properties);
  }

  public Locale getLocaleInstance() {
    lock.acquireReadLock();
    try {
      return (Locale) cache.get(LOCALE_PROPERTY_INSTANCE);
    } finally {
      lock.releaseReadLock();
    }
  }

  private void recalculateLocale() {
    Locale locale;
    try {
      final String localeLanguage = getLocaleLanguage();
      final String localeCountry = getLocaleCountry();

      if (localeLanguage == null || localeCountry == null) {
        locale = Locale.getDefault();
      } else {
        locale = new Locale(getLocaleLanguage(), getLocaleCountry());
      }
    } catch (final RuntimeException e) {
      locale = Locale.getDefault();
    }

    cache.put(LOCALE_PROPERTY_INSTANCE, locale);
  }

  @Override
  public boolean isStrictSql() {
    return true;
  }

  public void clearProperties() {
    lock.acquireWriteLock();
    try {
      final Stream<ORawPair<String, ORID>> stream = btree.iterateEntriesMajor(PROPERTY_PREFIX_PROPERTY, false, true);

      final List<String> keysToRemove = new ArrayList<>(8);
      final List<ORID> ridsToRemove = new ArrayList<>(8);

      stream.filter((entry) -> entry.first.startsWith(PROPERTY_PREFIX_PROPERTY)).forEach((entry) -> {
        keysToRemove.add(entry.first);
        ridsToRemove.add(entry.second);
      });

      boolean rollback = false;
      atomicOperationsManager.startAtomicOperation(COMPONENT_NAME, true);
      try {
        for (final String key : keysToRemove) {
          btree.remove(key);
        }

        for (final ORID rid : ridsToRemove) {
          cluster.deleteRecord(rid.getClusterPosition());
        }

        @SuppressWarnings("unchecked")
        final Map<String, String> properties = (Map<String, String>) cache.get(PROPERTIES);
        properties.clear();
      } catch (final Exception e) {
        rollback = true;
        throw e;
      } finally {
        atomicOperationsManager.endAtomicOperation(rollback);
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during clear of configuration properties"), e);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void removeProperty(final String name) {
    lock.acquireWriteLock();
    try {
      dropProperty(PROPERTY_PREFIX_PROPERTY + name);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void addIndexEngine(final String name, final IndexEngineData engineData) {
    lock.acquireWriteLock();
    try {
      final ORID identifiable = btree.get(ENGINE_PREFIX_PROPERTY + name);
      if (identifiable != null) {
        OLogManager.instance()
            .warn(this, "Index engine with name '" + engineData.getName() + "' already contained in database configuration");
      } else {
        storeProperty(ENGINE_PREFIX_PROPERTY + name, serializeIndexEngineProperty(engineData));
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void deleteIndexEngine(final String name) {
    lock.acquireWriteLock();
    try {
      dropProperty(ENGINE_PREFIX_PROPERTY + name);
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public Set<String> indexEngines() {
    lock.acquireReadLock();
    try {
      final Stream<ORawPair<String, ORID>> stream = btree.iterateEntriesMajor(ENGINE_PREFIX_PROPERTY, false, true);
      return stream.filter((entry) -> entry.first.startsWith(ENGINE_PREFIX_PROPERTY))
          .map((entry) -> entry.first.substring(ENGINE_PREFIX_PROPERTY.length())).collect(Collectors.toSet());
    } finally {
      lock.releaseReadLock();
    }
  }

  private List<IndexEngineData> loadIndexEngines() {
    final Stream<ORawPair<String, ORID>> stream = btree.iterateEntriesMajor(ENGINE_PREFIX_PROPERTY, false, true);
    return stream.filter((entry) -> entry.first.startsWith(ENGINE_PREFIX_PROPERTY)).map((entry) -> {
      String name = null;
      try {
        name = entry.first.substring(ENGINE_PREFIX_PROPERTY.length());
        final ORawBuffer buffer = cluster.readRecord(entry.second.getClusterPosition(), false);
        return deserializeIndexEngineProperty(name, buffer.buffer, Integer.MIN_VALUE);
      } catch (IOException e) {
        throw OException
            .wrapException(new OStorageException("Can not load data for index " + name + " for storage " + storage.getName()), e);
      }
    }).collect(Collectors.toList());
  }

  @Override
  public IndexEngineData getIndexEngine(final String name, int defaultIndexId) {
    lock.acquireReadLock();
    try {
      final byte[] property = readProperty(ENGINE_PREFIX_PROPERTY + name);
      if (property == null) {
        return null;
      }

      return deserializeIndexEngineProperty(name, property, defaultIndexId);
    } finally {
      lock.releaseReadLock();
    }
  }

  public void updateCluster(final OStorageClusterConfiguration config) {
    lock.acquireWriteLock();
    try {
      @SuppressWarnings("unchecked")
      final List<OStorageClusterConfiguration> clusters = (List<OStorageClusterConfiguration>) cache.get(CLUSTERS);
      if (config.getId() < clusters.size()) {
        clusters.set(config.getId(), config);
      } else {
        final int diff = config.getId() - clusters.size();
        for (int i = 0; i < diff; i++) {
          clusters.add(null);
        }

        clusters.add(config);
        assert clusters.size() - 1 == config.getId();
      }

      storeProperty(CLUSTERS_PREFIX_PROPERTY + config.getId(), updateClusterConfig(config));
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void setClusterStatus(final int clusterId, final OStorageClusterConfiguration.STATUS status) {
    lock.acquireWriteLock();
    try {
      @SuppressWarnings("unchecked")
      final List<OStorageClusterConfiguration> clusters = (List<OStorageClusterConfiguration>) cache.get(CLUSTERS);

      if (clusterId < clusters.size()) {
        final OStorageClusterConfiguration config = clusters.get(clusterId);
        config.setStatus(status);
      }

      final byte[] property = readProperty(CLUSTERS_PREFIX_PROPERTY + clusterId);
      if (property != null) {
        final OStorageClusterConfiguration clusterCfg = deserializeStorageClusterConfig(clusterId, property);
        clusterCfg.setStatus(status);
        updateCluster(clusterCfg);
      }
    } finally {
      lock.releaseWriteLock();
    }
  }

  @Override
  public List<OStorageClusterConfiguration> getClusters() {
    lock.acquireReadLock();
    try {
      //noinspection unchecked
      return Collections.unmodifiableList((List<OStorageClusterConfiguration>) cache.get(CLUSTERS));
    } finally {
      lock.releaseReadLock();
    }
  }

  private void preloadClusters() {
    final List<OStorageClusterConfiguration> clusters = new ArrayList<>(1024);
    final Stream<ORawPair<String, ORID>> stream = btree.iterateEntriesMajor(CLUSTERS_PREFIX_PROPERTY, false, true);

    stream.filter((entry) -> entry.first.startsWith(CLUSTERS_PREFIX_PROPERTY)).forEach((entry) -> {
      final int id = Integer.parseInt(entry.first.substring(CLUSTERS_PREFIX_PROPERTY.length()));

      try {
        final ORawBuffer buffer = cluster.readRecord(entry.second.getClusterPosition(), false);

        if (clusters.size() <= id) {
          final int diff = id - clusters.size();

          for (int i = 0; i < diff; i++) {
            clusters.add(null);
          }

          clusters.add(deserializeStorageClusterConfig(id, buffer.buffer));
          assert id == clusters.size() - 1;
        } else {
          clusters.set(id, deserializeStorageClusterConfig(id, buffer.buffer));
        }
      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Can not load data for cluster with id=" + id + " for storage " + storage.getName()), e);
      }

    });

    cache.put(CLUSTERS, clusters);
  }

  public void dropCluster(final int clusterId) {
    lock.acquireWriteLock();
    try {
      @SuppressWarnings("unchecked")
      final List<OStorageClusterConfiguration> clusters = (List<OStorageClusterConfiguration>) cache.get(CLUSTERS);
      if (clusterId < clusters.size()) {
        clusters.set(clusterId, null);
      }

      dropProperty(CLUSTERS_PREFIX_PROPERTY + clusterId);
    } finally {
      lock.releaseWriteLock();
    }
  }

  public void setConfigurationUpdateListener(final OStorageConfigurationUpdateListener updateListener) {
    lock.acquireWriteLock();
    try {
      this.updateListener = updateListener;
    } finally {
      lock.releaseWriteLock();
    }
  }

  private static byte[] serializeIndexEngineProperty(final IndexEngineData indexEngineData) {
    int totalSize = 0;
    final List<byte[]> entries = new ArrayList<>(16);

    final byte[] numericProperties = new byte[4 * OIntegerSerializer.INT_SIZE + 5 * OByteSerializer.BYTE_SIZE];
    totalSize += numericProperties.length;
    entries.add(numericProperties);

    {
      int pos = 0;
      OIntegerSerializer.INSTANCE.serializeNative(indexEngineData.getVersion(), numericProperties, pos);
      pos += OIntegerSerializer.INT_SIZE;
      OIntegerSerializer.INSTANCE.serializeNative(indexEngineData.getApiVersion(), numericProperties, pos);
      pos += OIntegerSerializer.INT_SIZE;

      numericProperties[pos] = indexEngineData.getValueSerializerId();
      pos++;
      numericProperties[pos] = indexEngineData.getKeySerializedId();
      pos++;
      numericProperties[pos] = indexEngineData.isAutomatic() ? (byte) 1 : 0;
      pos++;
      numericProperties[pos] = indexEngineData.isNullValuesSupport() ? (byte) 1 : 0;
      pos++;
      numericProperties[pos] = indexEngineData.isMultivalue() ? (byte) 1 : 0;
      pos++;

      OIntegerSerializer.INSTANCE.serializeNative(indexEngineData.getKeySize(), numericProperties, pos);
      pos += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(indexEngineData.getIndexId(), numericProperties, pos);
    }

    final byte[] algorithm = serializeStringValue(indexEngineData.getAlgorithm());
    totalSize += algorithm.length;
    entries.add(algorithm);

    final byte[] indexType = serializeStringValue(indexEngineData.getIndexType() == null ? "" : indexEngineData.getIndexType());
    entries.add(indexType);
    totalSize += indexType.length;

    final byte[] encryption = serializeStringValue(indexEngineData.getEncryption());
    totalSize += encryption.length;
    entries.add(encryption);

    final OType[] keyTypesValue = indexEngineData.getKeyTypes();
    final byte[] keyTypesSize = new byte[4];
    OIntegerSerializer.INSTANCE.serializeNative(keyTypesValue.length, keyTypesSize, 0);
    totalSize += keyTypesSize.length;
    entries.add(keyTypesSize);

    for (final OType typeValue : keyTypesValue) {
      final byte[] keyTypeName = serializeStringValue(typeValue.name());
      totalSize += keyTypeName.length;
      entries.add(keyTypeName);
    }

    final Map<String, String> engineProperties = indexEngineData.getEngineProperties();
    final byte[] enginePropertiesSize = new byte[OIntegerSerializer.INT_SIZE];
    totalSize += enginePropertiesSize.length;
    entries.add(enginePropertiesSize);

    if (engineProperties != null) {
      OIntegerSerializer.INSTANCE.serializeNative(engineProperties.size(), enginePropertiesSize, 0);

      for (final Map.Entry<String, String> engineProperty : engineProperties.entrySet()) {
        final byte[] key = serializeStringValue(engineProperty.getKey());
        totalSize += key.length;
        entries.add(key);

        final byte[] value = serializeStringValue(engineProperty.getValue());
        totalSize += value.length;
        entries.add(value);
      }
    }

    return mergeBinaryEntries(totalSize, entries);
  }

  private IndexEngineData deserializeIndexEngineProperty(final String name, final byte[] property, final int defaultIndexId) {
    int pos = 0;

    final int version = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    final int apiVersion = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    final byte valueSerializerId = property[pos];
    pos++;

    final byte keySerializerId = property[pos];
    pos++;

    final boolean isAutomatic = property[pos] == 1;
    pos++;

    final boolean isNullValueSupport = property[pos] == 1;
    pos++;

    final boolean isMultiValue = property[pos] == 1;
    pos++;

    final int keySize = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    final int indexId;
    if (getVersion() >= 23) {
      indexId = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
      pos += OIntegerSerializer.INT_SIZE;
    } else {
      indexId = defaultIndexId;
    }

    final String algorithm = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final String indexType = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final String encryption = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final int keyTypesSize = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    final OType[] keyTypes = new OType[keyTypesSize];
    for (int i = 0; i < keyTypesSize; i++) {
      final String typeName = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      keyTypes[i] = OType.valueOf(typeName);
    }

    final Map<String, String> engineProperties = new HashMap<>(8);
    final int enginePropertiesSize = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < enginePropertiesSize; i++) {
      final String key = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      final String value = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      engineProperties.put(key, value);
    }

    return new IndexEngineData(indexId, name, algorithm, indexType, true, version, apiVersion, isMultiValue, valueSerializerId,
        keySerializerId, isAutomatic, keyTypes, isNullValueSupport, keySize, encryption,
        configuration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY), engineProperties);
  }

  private static byte[] mergeBinaryEntries(final int totalSize, final List<byte[]> entries) {
    final byte[] property = new byte[totalSize];
    int pos = 0;
    for (final byte[] entry : entries) {
      System.arraycopy(entry, 0, property, pos, entry.length);
      pos += entry.length;
    }

    assert pos == property.length;
    return property;
  }

  private static byte[] updateClusterConfig(final OStorageClusterConfiguration cluster) {
    int totalSize = 0;
    final List<byte[]> entries = new ArrayList<>(8);

    final byte[] name = serializeStringValue(cluster.getName());
    totalSize += name.length;
    entries.add(name);

    final OStoragePaginatedClusterConfiguration paginatedClusterConfiguration = (OStoragePaginatedClusterConfiguration) cluster;
    final byte[] numericData = new byte[OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE];
    totalSize += numericData.length;
    entries.add(numericData);

    numericData[0] = paginatedClusterConfiguration.useWal ? (byte) 1 : 0;

    OIntegerSerializer.INSTANCE.serializeNative(paginatedClusterConfiguration.getBinaryVersion(), numericData, 1);

    final byte[] encryption = serializeStringValue(paginatedClusterConfiguration.encryption);
    totalSize += encryption.length;
    entries.add(encryption);

    final byte[] conflictStrategy = serializeStringValue(paginatedClusterConfiguration.conflictStrategy);
    totalSize += conflictStrategy.length;
    entries.add(conflictStrategy);

    final byte[] status = serializeStringValue(paginatedClusterConfiguration.getStatus().name());
    totalSize += status.length;
    entries.add(status);

    final byte[] compression = serializeStringValue(paginatedClusterConfiguration.compression);
    entries.add(compression);
    totalSize += compression.length;

    return mergeBinaryEntries(totalSize, entries);
  }

  private OStorageClusterConfiguration deserializeStorageClusterConfig(final int id, final byte[] property) {
    int pos = 0;

    final String name = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final boolean useWal = (property[pos] == 1);
    pos++;

    final int binaryVersion = OIntegerSerializer.INSTANCE.deserializeNative(property, pos);
    pos += OIntegerSerializer.INT_SIZE;

    final String encryption = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final String conflictStrategy = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final String status = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final String compression = deserializeStringValue(property, pos);

    return new OStoragePaginatedClusterConfiguration(id, name, null, useWal, 0, 0, compression, encryption,
        configuration.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY), conflictStrategy,
        OStorageClusterConfiguration.STATUS.valueOf(status), binaryVersion);
  }

  private void dropProperty(final String name) {
    try {
      boolean rollback = false;
      atomicOperationsManager.startAtomicOperation(COMPONENT_NAME, true);
      try {
        final ORID identifiable = btree.remove(name);

        if (identifiable != null) {
          cluster.deleteRecord(identifiable.getClusterPosition());
        }
      } catch (final Exception e) {
        rollback = true;
        throw e;
      } finally {
        atomicOperationsManager.endAtomicOperation(rollback);
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during drop of property " + name), e);
    }

    final PausedNotificationsState pausedNotificationsState = pauseNotifications.get();
    if (updateListener != null) {
      if (!pausedNotificationsState.notificationsPaused) {
        updateListener.onUpdate(this);
        pausedNotificationsState.pendingChanges = 0;
      } else {
        pausedNotificationsState.pendingChanges++;
      }
    }
  }

  private void updateStringProperty(final String name, final String value, final boolean useCache) {
    if (useCache) {
      cache.put(name, value);
    }

    final byte[] property = serializeStringValue(value);

    storeProperty(name, property);
  }

  private static byte[] serializeStringValue(final String value) {
    final byte[] property;
    if (value == null) {
      property = new byte[1];
    } else {
      final byte[] rawString = value.getBytes(StandardCharsets.UTF_16);
      property = new byte[rawString.length + 1 + OIntegerSerializer.INT_SIZE];
      property[0] = 1;

      OIntegerSerializer.INSTANCE.serializeNative(rawString.length, property, 1);

      System.arraycopy(rawString, 0, property, 5, rawString.length);
    }

    return property;
  }

  private static String deserializeStringValue(final byte[] raw, final int start) {
    if (raw[start] == 0) {
      return null;
    }

    final int stringSize = OIntegerSerializer.INSTANCE.deserializeNative(raw, start + 1);
    return new String(raw, start + 5, stringSize, StandardCharsets.UTF_16);
  }

  private static int getSerializedStringSize(final byte[] raw, final int start) {
    if (raw[start] == 0) {
      return 1;
    }

    return OIntegerSerializer.INSTANCE.deserializeNative(raw, start + 1) + 5;
  }

  private void updateIntProperty(final String name, final int value) {
    cache.put(name, value);

    final byte[] property = new byte[OIntegerSerializer.INT_SIZE];
    OIntegerSerializer.INSTANCE.serializeNative(value, property, 0);

    storeProperty(name, property);
  }

  private void storeProperty(final String name, final byte[] property) {
    try {
      boolean rollback = false;
      atomicOperationsManager.startAtomicOperation(COMPONENT_NAME, true);
      try {
        ORID identity = btree.get(name);
        if (identity == null) {
          final OPhysicalPosition position = cluster.createRecord(property, 0, (byte) 0, null);
          identity = new ORecordId(0, position.clusterPosition);
          btree.put(name, identity);
        } else {
          cluster.updateRecord(identity.getClusterPosition(), property, -1, (byte) 0);
        }
      } catch (final Exception e) {
        rollback = true;
        throw e;
      } finally {
        atomicOperationsManager.endAtomicOperation(rollback);
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during update of configuration property " + name), e);
    }

    final PausedNotificationsState pausedNotificationsState = pauseNotifications.get();
    if (updateListener != null) {
      if (!pausedNotificationsState.notificationsPaused) {
        pausedNotificationsState.pendingChanges = 0;
        updateListener.onUpdate(this);
      } else {
        pausedNotificationsState.pendingChanges++;
      }
    }
  }

  private byte[] readProperty(final String name) {
    try {
      final ORID rid = btree.get(name);
      if (rid == null) {
        return null;
      }

      final ORawBuffer buffer = cluster.readRecord(rid.getClusterPosition(), false);
      return buffer.buffer;
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during read of configuration property " + name), e);
    }
  }

  private boolean containsProperty(@SuppressWarnings("SameParameterValue") final String name) {
    return btree.get(name) != null;
  }

  private String readStringProperty(final String name) {
    return (String) cache.get(name);
  }

  private int readIntProperty(final String name, final boolean useCache) {
    if (useCache) {
      final Object cachedValue = cache.get(name);
      return (int) cachedValue;
    }

    final byte[] property = readProperty(name);
    if (property == null) {
      throw new IllegalStateException("Property " + name + " is absent");
    }

    if (property.length < 4) {
      throw new IllegalStateException("Invalid length of property " + name + " len = " + property.length);
    }

    return OIntegerSerializer.INSTANCE.deserializeNative(property, 0);
  }

  private void preloadIntProperties() {
    for (final String name : INT_PROPERTIES) {
      final byte[] property = readProperty(name);

      if (property != null) {
        cache.put(name, OIntegerSerializer.INSTANCE.deserializeNative(property, 0));
      }
    }
  }

  private void preloadStringProperties() {
    for (final String name : STRING_PROPERTIES) {
      final byte[] property = readProperty(name);

      if (property != null) {
        cache.put(name, deserializeStringValue(property, 0));
      }
    }
  }

  private void init() {
    updateVersion();
    updateBinaryFormatVersion();

    setCharset(DEFAULT_CHARSET);
    setDateFormat(DEFAULT_DATE_FORMAT);
    setDateTimeFormat(DEFAULT_DATETIME_FORMAT);
    setLocaleLanguage(Locale.getDefault().getLanguage());
    setLocaleCountry(Locale.getDefault().getCountry());
    setTimeZone(TimeZone.getDefault());

    setPageSize(-1);
    setFreeListBoundary(-1);
    setMaxKeySize(-1);

    if (!configuration.getContextKeys().contains(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.getKey())) {
      configuration.setValue(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS,
          OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.getValueAsInteger()); // 0 = AUTOMATIC
    }
    autoInitClusters();

    updateMinimumClusters();//store inside of configuration

    setRecordSerializerVersion(0);
  }

  private void copy(final OStorageConfiguration storageConfiguration) {
    updateVersion(storageConfiguration.getVersion());
    updateBinaryFormatVersion(storageConfiguration.getBinaryFormatVersion());

    setCharset(storageConfiguration.getCharset());
    setSchemaRecordId(storageConfiguration.getSchemaRecordId());
    setIndexMgrRecordId(storageConfiguration.getIndexMgrRecordId());

    final TimeZone timeZone = storageConfiguration.getTimeZone();
    assert timeZone != null;

    setTimeZone(timeZone);
    setDateFormat(storageConfiguration.getDateFormat());
    setDateTimeFormat(storageConfiguration.getDateTimeFormat());

    this.configuration = storageConfiguration.getContextConfiguration();

    setMinimumClusters(storageConfiguration.getMinimumClusters());

    setLocaleCountry(storageConfiguration.getLocaleCountry());
    setLocaleLanguage(storageConfiguration.getLocaleLanguage());

    final List<OStorageEntryConfiguration> properties = storageConfiguration.getProperties();
    for (final OStorageEntryConfiguration property : properties) {
      setProperty(property.name, property.value);
    }

    setClusterSelection(storageConfiguration.getClusterSelection());
    setConflictStrategy(storageConfiguration.getConflictStrategy());
    setValidation(storageConfiguration.isValidationEnabled());

    int counter = 0;
    final Set<String> indexEngines = storageConfiguration.indexEngines();

    for (final String engine : indexEngines) {
      addIndexEngine(engine, storageConfiguration.getIndexEngine(engine, counter));
      counter++;
    }

    setRecordSerializer(storageConfiguration.getRecordSerializer());
    setRecordSerializerVersion(storageConfiguration.getRecordSerializerVersion());

    final List<OStorageClusterConfiguration> clusters = storageConfiguration.getClusters();
    for (final OStorageClusterConfiguration cluster : clusters) {
      if (cluster != null) {
        updateCluster(cluster);
      }
    }

    setCreationVersion(storageConfiguration.getCreatedAtVersion());
    setPageSize(storageConfiguration.getPageSize());
    setFreeListBoundary(storageConfiguration.getFreeListBoundary());
    setMaxKeySize(storageConfiguration.getMaxKeySize());
  }

  private void autoInitClusters() {
    if (getContextConfiguration().getValueAsInteger(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS) == 0) {
      @SuppressWarnings("SpellCheckingInspection")
      final int cpus = Runtime.getRuntime().availableProcessors();
      getContextConfiguration().setValue(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, cpus > 64 ? 64 : cpus);
    }
  }

  private static final class PausedNotificationsState {
    private boolean notificationsPaused;
    private long    pendingChanges;
  }
}
