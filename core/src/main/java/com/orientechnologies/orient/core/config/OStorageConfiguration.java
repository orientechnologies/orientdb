package com.orientechnologies.orient.core.config;

import com.orientechnologies.orient.core.id.OImmutableRecordId;
import com.orientechnologies.orient.core.id.ORecordId;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

public interface OStorageConfiguration {
  ORecordId CONFIG_RID = new OImmutableRecordId(0, 0);

  String DEFAULT_CHARSET         = "UTF-8";
  String DEFAULT_DATE_FORMAT     = "yyyy-MM-dd";
  String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  int CURRENT_VERSION               = 18;
  int CURRENT_BINARY_FORMAT_VERSION = 13;

  int getBinaryFormatVersion();

  String getConflictStrategy();

  Set<String> indexEngines();

  OContextConfiguration getContextConfiguration();

  List<OStorageClusterConfiguration> getClusters();

  Locale getLocaleInstance();

  OStorageConfigurationImpl.IndexEngineData getIndexEngine(String name);

  int getVersion();

  String getDirectory();

  String getCharset();

  SimpleDateFormat getDateTimeFormatInstance();

  SimpleDateFormat getDateFormatInstance();

  String getDateFormat();

  String getDateTimeFormat();

  TimeZone getTimeZone();

  String getLocaleCountry();

  String getLocaleLanguage();

  List<OStorageEntryConfiguration> getProperties();

  String getClusterSelection();

  int getMinimumClusters();

  boolean isValidationEnabled();

  String getRecordSerializer();

  int getRecordSerializerVersion();

  String getSchemaRecordId();

  boolean isStrictSql();

  String getIndexMgrRecordId();

  boolean isTxRequiredForSQLGraphOperations();

  String getName();

  String getDictionaryRecordId();

  String getProperty(final String name);
}
