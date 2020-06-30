package com.orientechnologies.orient.test.util;

import java.io.IOException;

public class TestSetupUtil {
  public static TestSetup create(TestConfig TestConfig) throws IOException {
    String kubeConfigFile = System.getProperty("kube.config");
    if (kubeConfigFile == null) {
      System.out.println("Running with local JVMs");
      return new LocalSetup(TestConfig);
    }
    System.out.println("Running with Kube Config file " + kubeConfigFile);
    return new KubernetesSetup(kubeConfigFile, TestConfig);
  }
}
