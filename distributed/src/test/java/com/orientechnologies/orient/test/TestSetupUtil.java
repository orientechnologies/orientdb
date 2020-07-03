package com.orientechnologies.orient.test;

import io.kubernetes.client.openapi.StringUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class TestSetupUtil {
  private static final K8sServerConfig templateConfigs;
  private static final String          orientDBKubernetesLabel;

  static {
    templateConfigs = readTemplateConfigs();
    orientDBKubernetesLabel = System.getProperty("orientdbLabel");
  }

  public static TestSetup create(TestConfig TestConfig) throws IOException {
    String kubeConfigFile = System.getProperty("kubeConfig");
    if (kubeConfigFile == null) {
      System.out.println("Running with local JVMs");
      return new LocalSetup(TestConfig);
    }
    System.out.println("Running with Kube Config file " + kubeConfigFile);
    return new KubernetesSetup(kubeConfigFile, TestConfig);
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

  public static String readAllLines(String resourceFileName)
      throws URISyntaxException, IOException {
    List<String> lines =
        Files.readAllLines(Paths.get(ManifestTemplate.class.getResource(resourceFileName).toURI()));
    return StringUtil.join(lines.toArray(new String[] {}), "\r\n");
  }
}
