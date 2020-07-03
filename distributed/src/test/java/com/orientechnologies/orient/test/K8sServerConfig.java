package com.orientechnologies.orient.test;

public class K8sServerConfig {

  private String nodeName;
  private String httpPort;
  private String binaryPort;
  private String hazelcastPort;
  private String dockerImage;
  private String serverConfig;
  private String hazelcastConfig;
  private String distributedDBConfig;
  private String dbVolumeSize;
  // Following two are set after successful deployment.
  private String httpAddress;
  private String binaryAddress;

  public void validate() throws TestSetupException {
    String missingField = null;
    if (notSet(nodeName)) missingField = "nodeName";
    if (notSet(httpPort)) missingField = "httpPort";
    if (notSet(binaryPort)) missingField = "binaryPort";
    if (notSet(hazelcastPort)) missingField = "hazelcastPort";
    if (notSet(dockerImage)) missingField = "dockerImage";
    if (notSet(serverConfig)) missingField = "serverConfig";
    if (notSet(hazelcastConfig)) missingField = "hazelcastConfig";
    if (notSet(distributedDBConfig)) missingField = "distributedDBConfig";
    if (notSet(dbVolumeSize)) missingField = "dbVolumeSize";
    if (missingField != null) {
      throw new TestSetupException(
          "Missing value '" + missingField + "' in Kubernetes configuration for server.");
    }
  }

  private boolean notSet(String s) {
    return s == null || s.trim().equals("");
  }

  public K8sServerConfig() {}

  public K8sServerConfig(K8sServerConfig configs) {
    this.nodeName = configs.nodeName;
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
