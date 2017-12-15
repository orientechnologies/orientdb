package com.orientechnologies.orient.core.config;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public interface OStorageConfiguration {
  String DEFAULT_CHARSET               = "UTF-8";
  String DEFAULT_DATE_FORMAT           = "yyyy-MM-dd";
  String DEFAULT_DATETIME_FORMAT       = "yyyy-MM-dd HH:mm:ss";
  int    CURRENT_VERSION               = 17;
  int    CURRENT_BINARY_FORMAT_VERSION = 12;

  SimpleDateFormat getDateTimeFormatInstance();

  SimpleDateFormat getDateFormatInstance();

  String getCharset();

  Locale getLocaleInstance();

  String getSchemaRecordId();

  void setSchemaRecordId(String s);

  void update();

  int getMinimumClusters();

  boolean isStrictSql();

  OStorageConfiguration load(OContextConfiguration contextConfiguration);

  String getIndexMgrRecordId();

  void setIndexMgrRecordId(String indexMgrRecordId);

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

  String getRecordSerializer();

  int getRecordSerializerVersion();

  int getBinaryFormatVersion();

  void dropCluster(int iClusterId);

  int getVersion();

  String getName();

  String getProperty(String graphConsistencyMode);

  String getDirectory();

  List<OStorageClusterConfiguration> getClusters();
}
