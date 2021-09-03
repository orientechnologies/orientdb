package com.orientechnologies.orient.setup;

import io.kubernetes.client.openapi.StringUtil;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.joda.time.DateTime;

public class TestSetupUtil {
  private static final K8sServerConfig templateConfigs;
  // Following properties are not server-specific
  private static final String orientDBKubernetesLabel;
  private static final String configVolumeStorageClass;
  private static final String databaseVolumeStorageClass;
  private static final String kubernetesNamespace;

  static {
    templateConfigs = readTemplateConfigs();
    orientDBKubernetesLabel = System.getProperty("orientdbLabel", "");
    configVolumeStorageClass = System.getProperty("configVolumeStorageClass", "");
    databaseVolumeStorageClass = System.getProperty("databaseVolumeStorageClass", "");
    kubernetesNamespace = System.getProperty("kubernetesNamespace", "default");
  }

  public static TestSetup create(SetupConfig SetupConfig) throws IOException {
    String kubeConfigFile = System.getProperty("kubeConfig");
    if (kubeConfigFile == null) {
      log("Running with local JVMs.");
      return new LocalTestSetup(SetupConfig);
    }
    log(
        "Using template values: {"
            + "'HTTP port': '%s', "
            + "'binary port': '%s', "
            + "'Hazelcast port': '%s', "
            + "'Docker image': '%s', "
            + "'DB volume size': %s, "
            + "'OrientDB label': '%s', "
            + "'config volume storage class': '%s', "
            + "'DB volume storage class': '%s', "
            + "'Kubernetes namespace': '%s'}",
        templateConfigs.getHttpPort(),
        templateConfigs.getBinaryPort(),
        templateConfigs.getHazelcastPort(),
        templateConfigs.getDockerImage(),
        templateConfigs.getDbVolumeSize(),
        orientDBKubernetesLabel,
        configVolumeStorageClass,
        databaseVolumeStorageClass,
        kubernetesNamespace);
    log("Running with Kube Config file " + kubeConfigFile);
    return new KubernetesTestSetup(kubeConfigFile, SetupConfig);
  }

  public static K8sServerConfig newK8sConfigs() {
    return new K8sServerConfig(templateConfigs);
  }

  private static K8sServerConfig readTemplateConfigs() {
    K8sServerConfig config = new K8sServerConfig();
    config.setHttpPort(System.getProperty("orientdbHttpPort"));
    config.setBinaryPort(System.getProperty("orientdbBinaryPort"));
    config.setHazelcastPort(System.getProperty("orientdbHazelcastPort"));
    config.setDockerImage(System.getProperty("orientdbDockerImage"));
    config.setDbVolumeSize(System.getProperty("orientdbVolumeSize"));
    return config;
  }

  public static String getOrientDBKubernetesLabel() {
    return orientDBKubernetesLabel;
  }

  public static String getConfigVolumeStorageClass() {
    return configVolumeStorageClass;
  }

  public static String getDatabaseVolumeStorageClass() {
    return databaseVolumeStorageClass;
  }

  public static String getKubernetesNamespace() {
    return kubernetesNamespace;
  }

  public static String readAllLines(String resourceFileName)
      throws URISyntaxException, IOException {
    List<String> lines =
        Files.readAllLines(Paths.get(ManifestTemplate.class.getResource(resourceFileName).toURI()));
    return StringUtil.join(lines.toArray(new String[] {}), "\r\n");
  }

  public static void log(String msg, Object... args) {
    System.out.printf("%s -- %s\n", DateTime.now(), String.format(msg, args));
  }

  public static void sleep(int seconds) {
    try {
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException e) {
    }
  }
}
