package com.orientechnologies.orient.core.config;

import java.io.IOException;
import java.util.Set;
import java.util.TimeZone;

public interface OStorageConfigurationModifiable extends OStorageConfiguration {
  void setSchemaRecordId(String s);

  void update();

  void setIndexMgrRecordId(String indexMgrRecordId);

  void setDateFormat(String stringValue);

  void setDateTimeFormat(String stringValue);

  void setTimeZone(TimeZone timeZoneValue);

  void setLocaleCountry(String stringValue);

  void setLocaleLanguage(String stringValue);

  void setCharset(String stringValue);

  void setClusterSelection(String stringValue);

  void setMinimumClusters(int i);

  void setConflictStrategy(String stringValue);

  void setValidation(boolean b);

  void setProperty(String iName, String iValue);

  void removeProperty(String iName);

  void clearProperties();

  void setRecordSerializer(String recordSerializer);

  void setRecordSerializerVersion(int currentVersion);

  void create() throws IOException;

  void setCreationVersion(String version);

  void dropCluster(int iClusterId);

  Set<String> indexEngines();

  OStorageConfigurationImpl.IndexEngineData getIndexEngine(String indexName);

  void setClusterStatus(int clusterId, OStorageClusterConfiguration.STATUS iStatus);

  void setConfigurationUpdateListener(OStorageConfigurationUpdateListener updateListener);

  void initConfiguration(OContextConfiguration contextConfiguration);
}
