package com.orientechnologies.orient.core.config;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public interface OStorageConfiguration {
  String DEFAULT_CHARSET = "UTF-8";
  String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
  String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  int CURRENT_VERSION = 23;
  int CURRENT_BINARY_FORMAT_VERSION = 13;

  SimpleDateFormat getDateTimeFormatInstance();

  SimpleDateFormat getDateFormatInstance();

  String getCharset();

  Locale getLocaleInstance();

  String getSchemaRecordId();

  int getMinimumClusters();

  boolean isStrictSql();

  String getIndexMgrRecordId();

  TimeZone getTimeZone();

  String getDateFormat();

  String getDateTimeFormat();

  OContextConfiguration getContextConfiguration();

  String getLocaleCountry();

  String getLocaleLanguage();

  List<OStorageEntryConfiguration> getProperties();

  String getClusterSelection();

  String getConflictStrategy();

  boolean isValidationEnabled();

  OStorageConfiguration.IndexEngineData getIndexEngine(String name, int defaultIndexId);

  String getRecordSerializer();

  int getRecordSerializerVersion();

  int getBinaryFormatVersion();

  int getVersion();

  String getName();

  String getProperty(String graphConsistencyMode);

  String getDirectory();

  List<OStorageClusterConfiguration> getClusters();

  String getCreatedAtVersion();

  Set<String> indexEngines();

  int getPageSize();

  int getFreeListBoundary();

  int getMaxKeySize();

  void setUuid(OAtomicOperation atomicOperation, final String uuid);

  String getUuid();

  final class IndexEngineData {
    private final int indexId;
    private final String name;
    private final String algorithm;
    private final String indexType;

    @Deprecated
    // Needed only for disk backward compatibility
    private final Boolean durableInNonTxMode;

    private final int version;
    private final int apiVersion;
    private final boolean multivalue;
    private final byte valueSerializerId;
    private final byte keySerializedId;
    private final boolean isAutomatic;
    private final OType[] keyTypes;
    private final boolean nullValuesSupport;
    private final int keySize;
    private final Map<String, String> engineProperties;
    private final String encryption;
    private final String encryptionOptions;

    public IndexEngineData(
        int indexId,
        final String name,
        final String algorithm,
        String indexType,
        final Boolean durableInNonTxMode,
        final int version,
        final int apiVersion,
        final boolean multivalue,
        final byte valueSerializerId,
        final byte keySerializedId,
        final boolean isAutomatic,
        final OType[] keyTypes,
        final boolean nullValuesSupport,
        final int keySize,
        final String encryption,
        final String encryptionOptions,
        final Map<String, String> engineProperties) {
      this.indexId = indexId;
      this.name = name;
      this.algorithm = algorithm;
      this.indexType = indexType;
      this.durableInNonTxMode = durableInNonTxMode;
      this.version = version;
      this.apiVersion = apiVersion;
      this.multivalue = multivalue;
      this.valueSerializerId = valueSerializerId;
      this.keySerializedId = keySerializedId;
      this.isAutomatic = isAutomatic;
      this.keyTypes = keyTypes;
      this.nullValuesSupport = nullValuesSupport;
      this.keySize = keySize;
      this.encryption = encryption;
      this.encryptionOptions = encryptionOptions;
      if (engineProperties == null) this.engineProperties = null;
      else this.engineProperties = new HashMap<>(engineProperties);
    }

    public int getIndexId() {
      return indexId;
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

    public int getApiVersion() {
      return apiVersion;
    }

    public boolean isMultivalue() {
      return multivalue;
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
      if (engineProperties == null) return null;

      return Collections.unmodifiableMap(engineProperties);
    }

    public String getIndexType() {
      return indexType;
    }
  }
}
