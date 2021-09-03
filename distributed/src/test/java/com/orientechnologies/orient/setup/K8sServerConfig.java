package com.orientechnologies.orient.setup;

// Configurations required to deploy an instance of OrientDB on Kubernetes.
public class K8sServerConfig {

  private String nodeName;
  private String httpPort;
  private String binaryPort;
  private String hazelcastPort;
  private String dockerImage;
  // Path to the config files (the config folder of OrientDB) that are mounted via a configMap.
  private String serverConfig;
  private String hazelcastConfig;
  private String distributedDBConfig;
  private String serverLogConfig;
  private String clientLogConfig;
  // Server user and password are required for setting up server and checking connection
  private String serverUser;
  private String serverPass;
  // Requested volume size for the database volume e.g. 2Gi
  private String dbVolumeSize;
  // Following two are set after successful deployment.
  private String httpAddress;
  private String binaryAddress;

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
    this.serverLogConfig = configs.serverLogConfig;
    this.clientLogConfig = configs.clientLogConfig;
    this.dbVolumeSize = configs.dbVolumeSize;
    this.httpAddress = configs.httpAddress;
    this.binaryAddress = configs.binaryAddress;
    this.serverUser = configs.serverUser;
    this.serverPass = configs.serverPass;
  }

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
    if (notSet(serverUser)) missingField = "serverUser";
    if (notSet(serverPass)) missingField = "serverPass";
    // server and client log property files are not mandatory.
    if (missingField != null) {
      throw new TestSetupException(
          "Missing value '" + missingField + "' in Kubernetes configuration for server.");
    }
  }

  private boolean notSet(String s) {
    return s == null || s.trim().equals("");
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

  public String getServerLogConfig() {
    return serverLogConfig;
  }

  public void setServerLogConfig(String serverLogConfig) {
    this.serverLogConfig = serverLogConfig;
  }

  public String getClientLogConfig() {
    return clientLogConfig;
  }

  public void setClientLogConfig(String clientLogConfig) {
    this.clientLogConfig = clientLogConfig;
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

  public String getServerUser() {
    return serverUser;
  }

  public void setServerUser(String serverUser) {
    this.serverUser = serverUser;
  }

  public String getServerPass() {
    return serverPass;
  }

  public void setServerPass(String serverPass) {
    this.serverPass = serverPass;
  }
}
