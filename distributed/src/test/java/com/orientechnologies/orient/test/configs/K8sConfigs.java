package com.orientechnologies.orient.test.configs;

public class K8sConfigs {
  private String nodeName;
  private String label;
  private String httpPort;
  private String binaryPort;
  private String hazelcastPort;
  private String dockerImage;
  private String serverConfig;
  private String hazelcastConfig;
  private String distributedDBConfig;
  private String dbVolumeSize;
  private String httpAddress;
  private String binaryAddress;

  public K8sConfigs() {}

  public K8sConfigs(K8sConfigs configs) {
    this.nodeName = configs.nodeName;
    this.label = configs.label;
    this.httpPort = configs.httpPort;
    this.binaryPort = configs.binaryPort;
    this.hazelcastPort = configs.hazelcastPort;
    this.dockerImage = configs.dockerImage;
    this.serverConfig = configs.serverConfig;
    this.hazelcastConfig = configs.hazelcastConfig;
    this.distributedDBConfig = configs.distributedDBConfig;
    this.dbVolumeSize = configs.dbVolumeSize;
    this.httpAddress = configs.httpAddress;
    this.binaryAddress = configs.binaryAddress;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(String httpPort) {
    this.httpPort = httpPort;
  }

  public String getBinaryPort() {
    return binaryPort;
  }

  public void setBinaryPort(String binaryPort) {
    this.binaryPort = binaryPort;
  }

  public String getHazelcastPort() {
    return hazelcastPort;
  }

  public void setHazelcastPort(String hazelcastPort) {
    this.hazelcastPort = hazelcastPort;
  }

  public String getDockerImage() {
    return dockerImage;
  }

  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public String getConfigMapName() {
    return nodeName + "-cm";
  }

  public String getServerConfig() {
    return serverConfig;
  }

  public void setServerConfig(String serverConfig) {
    this.serverConfig = serverConfig;
  }

  public String getHazelcastConfig() {
    return hazelcastConfig;
  }

  public void setHazelcastConfig(String hazelcastConfig) {
    this.hazelcastConfig = hazelcastConfig;
  }

  public String getDistributedDBConfig() {
    return distributedDBConfig;
  }

  public void setDistributedDBConfig(String distributedDBConfig) {
    this.distributedDBConfig = distributedDBConfig;
  }

  public String getDbVolumeSize() {
    return dbVolumeSize;
  }

  public void setDbVolumeSize(String dbVolumeSize) {
    this.dbVolumeSize = dbVolumeSize;
  }

  public String getHttpAddress() {
    return httpAddress;
  }

  public void setHttpAddress(String httpAddress) {
    this.httpAddress = httpAddress;
  }

  public String getBinaryAddress() {
    return binaryAddress;
  }

  public void setBinaryAddress(String binaryAddress) {
    this.binaryAddress = binaryAddress;
  }
}
