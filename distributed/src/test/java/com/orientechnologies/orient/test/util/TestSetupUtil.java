package com.orientechnologies.orient.test.util;

import com.orientechnologies.orient.test.configs.K8sConfigs;
import io.kubernetes.client.openapi.StringUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class TestSetupUtil {
  private static final K8sConfigs templateConfigs;

  static {
    templateConfigs = readTemplateConfigs();
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

  public static K8sConfigs newK8sConfigs() {
    return new K8sConfigs(templateConfigs);
  }

  private static K8sConfigs readTemplateConfigs() {
    K8sConfigs config = new K8sConfigs();
    config.setLabel(System.getProperty("orientdbLabel"));
    config.setHttpPort(System.getProperty("orientdbHttpPort"));
    config.setBinaryPort(System.getProperty("orientdbBinaryPort"));
    config.setHazelcastPort(System.getProperty("orientdbHazelcastPort"));
    config.setDockerImage(System.getProperty("orientdbDockerImage"));
    config.setDbVolumeSize(System.getProperty("orientdbVolumeSize"));
    return config;
  }

  public static String readAllLines(String resourceFileName)
      throws URISyntaxException, IOException {
    List<String> lines =
        Files.readAllLines(Paths.get(ManifestTemplate.class.getResource(resourceFileName).toURI()));
    return StringUtil.join(lines.toArray(new String[] {}), "\r\n");
  }
}
