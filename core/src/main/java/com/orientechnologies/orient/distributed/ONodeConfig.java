package com.orientechnologies.orient.distributed;

import com.orientechnologies.common.profiler.OProfilerEntrySnapshot;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ONodeConfig {

  private int id;
  private String uuid;
  private String name;
  private String version;
  private Date startedOn;
  private String status;
  private int connections;
  private Set<String> databases;
  private long usedMemory;
  private long freeMemory;
  private long maxMemory;
  private double cpu;
  private List<ONodeListenerConfig> listeners;
  private Map<String, String> databasesStatus;
  private List<ONodeLatencies> latencies;
  private List<ONodeMessages> messageStats;
  private ODocument configuration;

  public ONodeConfig(ODocument config) {
    id = config.getProperty("id");
    uuid = config.getProperty("uuid");
    name = config.getProperty("name");
    version = config.getProperty("version");
    startedOn = config.getProperty("startedOn");
    status = config.getProperty("status");
    connections = config.getProperty("connections");
    databases = config.getProperty("databases");
    usedMemory = config.getProperty("usedMemory");
    maxMemory = config.getProperty("maxMemory");
    freeMemory = config.getProperty("freeMemory");
    cpu = config.getProperty("cpu");
    List<Map<String, String>> listeners = config.getProperty("listeners");
    if (listeners != null) {
      this.listeners = listeners.stream().map(ONodeListenerConfig::new).toList();
    }
    databasesStatus = config.getProperty("databasesStatus");
    ODocument lat = config.getProperty("latencies");
    if (lat != null) {
      latencies = new ArrayList<>();
      for (var entry : lat)
        latencies.add(
            new ONodeLatencies(
                new ONodeId(entry.getKey()),
                new OProfilerEntrySnapshot((ODocument) entry.getValue())));
    }

    ODocument msgs = config.getProperty("messages");
    if (msgs != null) {
      messageStats = new ArrayList<>();
      for (var entry : msgs)
        messageStats.add(new ONodeMessages(entry.getKey(), (long) entry.getValue()));
    }
  }

  public ONodeConfig() {}

  public ODocument getConfig() {
    var config = new ODocument();
    config.setProperty("id", id);
    config.setProperty("uuid", uuid);
    config.setProperty("name", name);
    config.setProperty("version", version);
    config.setProperty("startedOn", startedOn);
    config.setProperty("status", status);
    config.setProperty("connections", connections);
    config.setProperty("databases", databases);
    config.setProperty("usedMemory", usedMemory);
    config.setProperty("maxMemory", maxMemory);
    config.setProperty("freeMemory", freeMemory);
    config.setProperty("cpu", cpu);
    if (listeners != null) {
      config.setProperty(
          "listeners",
          listeners.stream().map(ONodeListenerConfig::toMap).toList(),
          OType.EMBEDDEDLIST);
    }
    config.setProperty("databasesStatus", databasesStatus, OType.EMBEDDEDMAP);
    if (latencies != null) {
      var lat = new ODocument();
      for (var entry : latencies)
        lat.field(entry.node().getNode(), entry.stats().toDocument(), OType.EMBEDDED);
      config.setProperty("latencies", lat, OType.EMBEDDED);
    }
    if (messageStats != null) {
      var msgs = new ODocument();
      for (var entry : messageStats) msgs.field(entry.name(), entry.messages());

      config.setProperty("messages", msgs, OType.EMBEDDED);
    }
    return config;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Date getStartedOn() {
    return startedOn;
  }

  public void setStartedOn(Date startedOn) {
    this.startedOn = startedOn;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getConnections() {
    return connections;
  }

  public void setConnections(int connections) {
    this.connections = connections;
  }

  public Set<String> getDatabases() {
    return databases;
  }

  public void setDatabases(Set<String> databases) {
    this.databases = databases;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public void setUsedMemory(long usedMemory) {
    this.usedMemory = usedMemory;
  }

  public long getFreeMemory() {
    return freeMemory;
  }

  public void setFreeMemory(long freeMemory) {
    this.freeMemory = freeMemory;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public void setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public double getCpu() {
    return cpu;
  }

  public void setCpu(double cpu) {
    this.cpu = cpu;
  }

  public void setListeners(List<ONodeListenerConfig> listeners) {
    this.listeners = listeners;
  }

  public List<ONodeListenerConfig> getListeners() {
    return this.listeners;
  }

  public void setLatencies(List<ONodeLatencies> latencies) {
    this.latencies = latencies;
  }

  public List<ONodeLatencies> getLatencies() {
    return this.latencies;
  }

  public void setMessages(List<ONodeMessages> messageStats) {
    this.messageStats = messageStats;
  }

  public List<ONodeMessages> getMessages() {
    return messageStats;
  }

  public void setDatabasesStatus(Map<String, String> dbStatus) {
    this.databasesStatus = dbStatus;
  }

  public ODocument getConfiguration() {
    return this.configuration;
  }

  public void setConfiguration(ODocument configuration) {
    this.configuration = configuration;
  }
}
