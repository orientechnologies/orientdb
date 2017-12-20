package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.OReloadResponse37;
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.config.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class OStorageConfigurationRemote implements OStorageConfiguration {

  private OContextConfiguration contextConfiguration;

  private String                                  dateFormat;
  private String                                  dateTimeFormat;
  private String                                  name;
  private int                                     version;
  private String                                  directory;
  private Map<String, OStorageEntryConfiguration> properties;
  private String                                  schemaRecordId;
  private String                                  indexMgrRecordId;
  private String                                  clusterSelection;
  private String                                  conflictStrategy;
  private boolean                                 validationEnabled;
  private String                                  localeLanguage;
  private int                                     minimumClusters;
  private boolean                                 strictSql;
  private String                                  charset;
  private TimeZone                                timeZone;
  private String                                  localeCountry;
  private String                                  recordSerializer;
  private int                                     recordSerializerVersion;
  private int                                     binaryFormatVersion;
  private List<OStorageClusterConfiguration>      clusters;
  private String                                  networkRecordSerializer;

  public OStorageConfigurationRemote(String networkRecordSerializer, OStorageConfigurationPayload payload ,
      OContextConfiguration contextConfiguration) {
    this.networkRecordSerializer = networkRecordSerializer;
    this.contextConfiguration = contextConfiguration;
    this.dateFormat = payload.getDateFormat();
    this.dateTimeFormat = payload.getDateTimeFormat();
    this.name = payload.getName();
    this.version = payload.getVersion();
    this.directory = payload.getDirectory();
    this.properties = new HashMap<>();
    for (OStorageEntryConfiguration conf : payload.getProperties()) {
      this.properties.put(conf.name, conf);
    }
    this.schemaRecordId = payload.getSchemaRecordId().toString();
    this.indexMgrRecordId = payload.getIndexMgrRecordId().toString();
    this.clusterSelection = payload.getClusterSelection();
    this.conflictStrategy = payload.getConflictStrategy();
    this.validationEnabled = payload.isValidationEnabled();
    this.localeLanguage = payload.getLocaleLanguage();
    this.minimumClusters = payload.getMinimumClusters();
    this.strictSql = payload.isStrictSql();
    this.charset = payload.getCharset();
    this.timeZone = payload.getTimeZone();
    this.localeCountry = payload.getLocaleCountry();
    this.recordSerializer = payload.getRecordSerializer();
    this.recordSerializerVersion = payload.getRecordSerializerVersion();
    this.binaryFormatVersion = payload.getBinaryFormatVersion();
    this.clusters = payload.getClusters();
  }

  @Override
  public SimpleDateFormat getDateTimeFormatInstance() {
    return new SimpleDateFormat(dateTimeFormat);
  }

  @Override
  public SimpleDateFormat getDateFormatInstance() {
    return new SimpleDateFormat(dateFormat);
  }

  @Override
  public String getCharset() {
    return charset;
  }

  @Override
  public Locale getLocaleInstance() {
    return Locale.forLanguageTag(localeCountry);
  }

  @Override
  public String getSchemaRecordId() {
    return schemaRecordId;
  }

  @Override
  public void setSchemaRecordId(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void update() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMinimumClusters() {
    return minimumClusters;
  }

  @Override
  public boolean isStrictSql() {
    return strictSql;
  }

  @Override
  public OStorageConfiguration load(OContextConfiguration contextConfiguration) {
    this.contextConfiguration = contextConfiguration;
    return null;
  }

  @Override
  public String getIndexMgrRecordId() {
    return indexMgrRecordId;
  }

  @Override
  public void setIndexMgrRecordId(String indexMgrRecordId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public String getDateFormat() {
    return dateFormat;
  }

  @Override
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  @Override
  public OContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  @Override
  public String getLocaleCountry() {
    return localeCountry;
  }

  @Override
  public String getLocaleLanguage() {
    return localeLanguage;
  }

  @Override
  public List<OStorageEntryConfiguration> getProperties() {
    return new ArrayList<>(properties.values());
  }

  @Override
  public String getClusterSelection() {
    return clusterSelection;
  }

  @Override
  public String getConflictStrategy() {
    return conflictStrategy;
  }

  @Override
  public boolean isValidationEnabled() {
    return validationEnabled;
  }

  @Override
  public void setDateFormat(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDateTimeFormat(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTimeZone(TimeZone timeZoneValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLocaleCountry(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLocaleLanguage(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCharset(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClusterSelection(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMinimumClusters(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConflictStrategy(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValidation(boolean b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setProperty(String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeProperty(String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearProperties() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRecordSerializer() {
    return networkRecordSerializer;
  }

  @Override
  public int getRecordSerializerVersion() {
    return recordSerializerVersion;
  }

  @Override
  public int getBinaryFormatVersion() {
    return binaryFormatVersion;
  }

  @Override
  public void dropCluster(int iClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getProperty(String graphConsistencyMode) {
    return properties.get(graphConsistencyMode).value;
  }

  @Override
  public String getDirectory() {
    return directory;
  }

  @Override
  public List<OStorageClusterConfiguration> getClusters() {
    return clusters;
  }
}
