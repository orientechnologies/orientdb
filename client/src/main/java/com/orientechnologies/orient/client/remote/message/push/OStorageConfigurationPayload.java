package com.orientechnologies.orient.client.remote.message.push;

import com.orientechnologies.orient.client.remote.OStorageClusterConfigurationRemote;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class OStorageConfigurationPayload {
  private String dateFormat;
  private String dateTimeFormat;
  private String name;
  private int version;
  private String directory;
  private List<OStorageEntryConfiguration> properties;
  private ORecordId schemaRecordId;
  private ORecordId indexMgrRecordId;
  private String clusterSelection;
  private String conflictStrategy;
  private boolean validationEnabled;
  private String localeLanguage;
  private int minimumClusters;
  private boolean strictSql;
  private String charset;
  private TimeZone timeZone;
  private String localeCountry;
  private String recordSerializer;
  private int recordSerializerVersion;
  private int binaryFormatVersion;
  private List<OStorageClusterConfiguration> clusters;

  public OStorageConfigurationPayload(OStorageConfiguration configuration) {
    this.dateFormat = configuration.getDateFormat();
    this.dateTimeFormat = configuration.getDateTimeFormat();
    this.name = configuration.getName();
    this.version = configuration.getVersion();
    this.directory = configuration.getDirectory();
    this.properties = configuration.getProperties();
    this.schemaRecordId = new ORecordId(configuration.getSchemaRecordId());
    this.indexMgrRecordId = new ORecordId(configuration.getIndexMgrRecordId());
    this.clusterSelection = configuration.getClusterSelection();
    this.conflictStrategy = configuration.getConflictStrategy();
    this.validationEnabled = configuration.isValidationEnabled();
    this.localeLanguage = configuration.getLocaleLanguage();
    this.minimumClusters = configuration.getMinimumClusters();
    this.strictSql = configuration.isStrictSql();
    this.charset = configuration.getCharset();
    this.timeZone = configuration.getTimeZone();
    this.localeCountry = configuration.getLocaleCountry();
    this.recordSerializer = configuration.getRecordSerializer();
    this.recordSerializerVersion = configuration.getRecordSerializerVersion();
    this.binaryFormatVersion = configuration.getBinaryFormatVersion();
    this.clusters = new ArrayList<>();
    for (OStorageClusterConfiguration conf : configuration.getClusters()) {
      if (conf != null) {
        this.clusters.add(conf);
      }
    }
  }

  public OStorageConfigurationPayload() {}

  public void write(OChannelDataOutput channel) throws IOException {
    channel.writeString(this.dateFormat);
    channel.writeString(this.dateTimeFormat);
    channel.writeString(this.name);
    channel.writeInt(this.version);
    channel.writeString(this.directory);
    channel.writeInt(properties.size());
    for (OStorageEntryConfiguration property : properties) {
      channel.writeString(property.name);
      channel.writeString(property.value);
    }
    channel.writeRID(this.schemaRecordId);
    channel.writeRID(this.indexMgrRecordId);
    channel.writeString(this.clusterSelection);
    channel.writeString(this.conflictStrategy);
    channel.writeBoolean(this.validationEnabled);
    channel.writeString(this.localeLanguage);
    channel.writeInt(this.minimumClusters);
    channel.writeBoolean(this.strictSql);
    channel.writeString(this.charset);
    channel.writeString(this.timeZone.getID());
    channel.writeString(this.localeCountry);
    channel.writeString(this.recordSerializer);
    channel.writeInt(this.recordSerializerVersion);
    channel.writeInt(this.binaryFormatVersion);
    channel.writeInt(clusters.size());
    for (OStorageClusterConfiguration cluster : clusters) {
      channel.writeInt(cluster.getId());
      channel.writeString(cluster.getName());
    }
  }

  public void read(OChannelDataInput network) throws IOException {
    this.dateFormat = network.readString();
    this.dateTimeFormat = network.readString();
    this.name = network.readString();
    this.version = network.readInt();
    this.directory = network.readString();
    int propSize = network.readInt();
    properties = new ArrayList<>(propSize);
    while (propSize-- > 0) {
      String name = network.readString();
      String value = network.readString();
      properties.add(new OStorageEntryConfiguration(name, value));
    }
    this.schemaRecordId = network.readRID();
    this.indexMgrRecordId = network.readRID();
    this.clusterSelection = network.readString();
    this.conflictStrategy = network.readString();
    this.validationEnabled = network.readBoolean();
    this.localeLanguage = network.readString();
    this.minimumClusters = network.readInt();
    this.strictSql = network.readBoolean();
    this.charset = network.readString();
    this.timeZone = TimeZone.getTimeZone(network.readString());
    this.localeCountry = network.readString();
    this.recordSerializer = network.readString();
    this.recordSerializerVersion = network.readInt();
    this.binaryFormatVersion = network.readInt();
    int clustersSize = network.readInt();
    clusters = new ArrayList<>(clustersSize);
    while (clustersSize-- > 0) {
      int clusterId = network.readInt();
      String clusterName = network.readString();
      clusters.add(new OStorageClusterConfigurationRemote(clusterId, clusterName));
    }
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public String getName() {
    return name;
  }

  public int getVersion() {
    return version;
  }

  public String getDirectory() {
    return directory;
  }

  public List<OStorageEntryConfiguration> getProperties() {
    return properties;
  }

  public ORecordId getSchemaRecordId() {
    return schemaRecordId;
  }

  public ORecordId getIndexMgrRecordId() {
    return indexMgrRecordId;
  }

  public String getClusterSelection() {
    return clusterSelection;
  }

  public String getConflictStrategy() {
    return conflictStrategy;
  }

  public boolean isValidationEnabled() {
    return validationEnabled;
  }

  public String getLocaleLanguage() {
    return localeLanguage;
  }

  public int getMinimumClusters() {
    return minimumClusters;
  }

  public boolean isStrictSql() {
    return strictSql;
  }

  public String getCharset() {
    return charset;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public String getLocaleCountry() {
    return localeCountry;
  }

  public String getRecordSerializer() {
    return recordSerializer;
  }

  public int getRecordSerializerVersion() {
    return recordSerializerVersion;
  }

  public int getBinaryFormatVersion() {
    return binaryFormatVersion;
  }

  public List<OStorageClusterConfiguration> getClusters() {
    return clusters;
  }
}
