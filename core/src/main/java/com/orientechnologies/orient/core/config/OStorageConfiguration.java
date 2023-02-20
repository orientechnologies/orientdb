package com.orientechnologies.orient.core.config;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
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

  IndexEngineData getIndexEngine(String name, int defaultIndexId);

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
}
