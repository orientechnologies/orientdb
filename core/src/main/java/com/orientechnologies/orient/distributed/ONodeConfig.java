package com.orientechnologies.orient.distributed;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ONodeConfig {

  private final ODocument config;

  public ONodeConfig(ODocument config) {
    this.config = config;
  }

  public ONodeConfig() {
    this.config = new ODocument();
  }

  public ODocument getConfig() {
    return config;
  }

  public void setId(int nodeId) {
    config.setProperty("id", nodeId);
  }

  public Integer getId() {
    return config.getProperty("id");
  }

  public void setUuid(String nodeUuid) {
    config.setProperty("uuid", nodeUuid);
  }

  public void setName(String nodeName) {
    config.setProperty("name", nodeName);
  }

  public String getName() {
    return config.getProperty("name");
  }

  public void setVersion(String rawVersion) {
    config.setProperty("version", rawVersion);
  }

  public void setPublicAddress(String publicAddress) {
    config.setProperty("publicAddress", publicAddress);
  }

  public void setStartedOn(Date startedOn) {
    config.setProperty("startedOn", startedOn);
  }

  public Date getStartedOn() {
    return config.getProperty("startedOn");
  }

  public void setStatus(String nodeStatus) {
    config.setProperty("status", nodeStatus);
  }

  public String getStatus() {
    return config.getProperty("status");
  }

  public void setConnections(int total) {
    config.setProperty("connections", total);
  }

  public Integer getConnections() {
    return config.getProperty("connections");
  }

  public void setDatabases(Set<String> databases) {
    config.setProperty("databases", databases);
  }

  public Set<String> getDatabases() {
    return config.getProperty("databases");
  }

  public void setReplicator(String password) {
    config.setProperty("user_replicator", password);
  }

  public String getReplicator() {
    return config.getProperty("user_replicator");
  }

  public void setUsedMemory(long usedMem) {
    config.setProperty("usedMemory", usedMem);
  }

  public Long getUsedMemory() {
    return config.getProperty("usedMemory");
  }

  public void setFreeMemory(long freeMem) {
    config.setProperty("freeMemory", freeMem);
  }

  public void setMaxMemory(long maxMem) {
    config.setProperty("maxMemory", maxMem);
  }

  public long getMaxMemory() {
    return config.getProperty("maxMemory");
  }

  public void setCpu(double cpuUsage) {
    config.setProperty("cpu", cpuUsage);
  }

  public void setListeners(List<ONodeListenerConfig> listeners) {
    config.setProperty(
        "listeners", listeners.stream().map((x) -> x.toMap()).toList(), OType.EMBEDDEDLIST);
  }

  public List<ONodeListenerConfig> getListeners() {
    List<Map<String, String>> listeners = config.getProperty("listeners");
    if (listeners != null) {
      return listeners.stream().map((x) -> new ONodeListenerConfig(x)).toList();
    }
    return null;
  }

  public void setLatencies(String string, ODocument latencies) {
    config.setProperty("latencies", latencies, OType.EMBEDDED);
  }

  public ODocument getLatencies() {
    return config.getProperty("latencies");
  }

  public void setMessages(String string, ODocument messageStats) {
    config.setProperty("messages", messageStats, OType.EMBEDDED);
  }

  public ODocument getMessages() {
    return config.getProperty("messages");
  }

  public void setDatabasesStatus(Map<String, String> dbStatus) {
    config.setProperty("databasesStatus", dbStatus, OType.EMBEDDEDMAP);
  }

  public ODocument getConfiguration() {
    return config.getProperty("configuration");
  }

  public void setConfiguration(ODocument configuration) {
    config.setProperty("configuration", configuration, OType.EMBEDDED);
  }
}
