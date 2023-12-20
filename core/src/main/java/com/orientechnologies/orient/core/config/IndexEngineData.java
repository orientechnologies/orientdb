package com.orientechnologies.orient.core.config;

import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class IndexEngineData {
  private final int indexId;
  private final String name;
  private final String algorithm;
  private final String indexType;

  @Deprecated
  // Needed only for disk backward compatibility
  private final Boolean durableInNonTxMode;

  private final int version;

  @Deprecated
  // Needed only for disk backward compatibility
  private int apiVersion = 1;

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
      final OIndexMetadata metadata,
      final Boolean durableInNonTxMode,
      final byte valueSerializerId,
      final byte keySerializedId,
      final OType[] keyTypes,
      final int keySize,
      final String encryption,
      final String encryptionOptions,
      final Map<String, String> engineProperties) {
    this.indexId = indexId;
    OIndexDefinition definition = metadata.getIndexDefinition();
    this.name = metadata.getName();
    this.algorithm = metadata.getAlgorithm();
    this.indexType = metadata.getType();
    this.durableInNonTxMode = durableInNonTxMode;
    this.version = metadata.getVersion();
    this.multivalue = metadata.isMultivalue();
    this.valueSerializerId = valueSerializerId;
    this.keySerializedId = keySerializedId;
    this.isAutomatic = definition.isAutomatic();
    this.keyTypes = keyTypes;
    this.nullValuesSupport = !definition.isNullValuesIgnored();
    this.keySize = keySize;
    this.encryption = encryption;
    this.encryptionOptions = encryptionOptions;
    if (engineProperties == null) this.engineProperties = null;
    else this.engineProperties = new HashMap<>(engineProperties);
  }

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

  @Deprecated
  public Boolean getDurableInNonTxMode() {
    return durableInNonTxMode;
  }

  public int getVersion() {
    return version;
  }

  @Deprecated
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
