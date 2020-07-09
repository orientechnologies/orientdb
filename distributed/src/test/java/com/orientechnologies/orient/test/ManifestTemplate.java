package com.orientechnologies.orient.test;

import org.apache.commons.text.StringSubstitutor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ManifestTemplate {
  public static final String ORIENTDB_NODE_NAME = "orientdbNodeName";
  public static final String ORIENTDB_DB_VOL_SIZE = "databaseVolSize";
  public static final String ORIENTDB_LABEL = "orientdbLabel";
  public static final String ORIENTDB_HTTP_PORT = "orientdbHttpPort";
  public static final String ORIENTDB_BINARY_PORT = "orientdbBinaryPort";
  public static final String ORIENTDB_HAZELCAST_PORT = "orientdbHazelcastPort";
  public static final String ORIENTDB_DOCKER_IMAGE = "orientdbDockerImage";
  public static final String ORIENTDB_CONFIGMAP_NAME = "orientdbConfigCm";
  public static final String CONFIG_VOLUME_STORAGE_CLASS = "configVolumeStorageClass";
  public static final String DATABASE_VOLUME_STORAGE_CLASS = "databaseVolumeStorageClass";

  public static final String ORIENTDB_HEADLESS_SERVICE_TEMPLATE =
      "/kubernetes/manifests/orientdb-headless-service.template.yaml";
  public static final String ORIENTDB_NODEPORT_SERVICE_TEMPLATE =
      "/kubernetes/manifests/orientdb-nodeport-service.template.yaml";
  public static final String ORIENTDB_STATEFULSET_TEMPLATE =
      "/kubernetes/manifests/orientdb-statefulset.template.yaml";

  public static String generateStatefulSet(K8sServerConfig configs)
      throws IOException, URISyntaxException {
    return generateManifest(configs, ORIENTDB_STATEFULSET_TEMPLATE);
  }

  public static String generateHeadlessService(K8sServerConfig configs)
      throws IOException, URISyntaxException {
    return generateManifest(configs, ORIENTDB_HEADLESS_SERVICE_TEMPLATE);
  }

  public static String generateNodePortService(K8sServerConfig configs)
      throws IOException, URISyntaxException {
    return generateManifest(configs, ORIENTDB_NODEPORT_SERVICE_TEMPLATE);
  }

  public static String generateManifest(K8sServerConfig configs, String templateFile)
      throws IOException, URISyntaxException {
    StringSubstitutor substitutor = new StringSubstitutor(createSubstitutorValueMap(configs));
    String template = TestSetupUtil.readAllLines(templateFile);
    return substitutor.replace(template);
  }

  public static Map<String, String> createSubstitutorValueMap(K8sServerConfig config) {
    return new HashMap<String, String>() {
      {
        put(ORIENTDB_NODE_NAME, config.getNodeName());
        put(ORIENTDB_DB_VOL_SIZE, config.getDbVolumeSize());
        put(ORIENTDB_LABEL, TestSetupUtil.getOrientDBKubernetesLabel());
        put(ORIENTDB_HTTP_PORT, config.getHttpPort());
        put(ORIENTDB_BINARY_PORT, config.getBinaryPort());
        put(ORIENTDB_HAZELCAST_PORT, config.getHazelcastPort());
        put(ORIENTDB_DOCKER_IMAGE, config.getDockerImage());
        put(ORIENTDB_CONFIGMAP_NAME, config.getConfigMapName());
        put(CONFIG_VOLUME_STORAGE_CLASS, TestSetupUtil.getConfigVolumeStorageClass());
        put(DATABASE_VOLUME_STORAGE_CLASS, TestSetupUtil.getDatabaseVolumeStorageClass());
      }
    };
  }
}
