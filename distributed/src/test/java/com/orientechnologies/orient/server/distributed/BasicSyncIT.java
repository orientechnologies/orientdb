package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.testutil.StatefulSetTemplateKeys;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.StringUtil;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import org.apache.commons.text.StringSubstitutor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BasicSyncIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;
  private List<String> PvcToDelete = new ArrayList<>();

  @BeforeClass
  public static void setupKubernetesClient() throws IOException {
    String kubeConfigFilePath = "/home/pxsalehi/.kube/config";
    ApiClient client =
        ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigFilePath)))
            .build();
    Configuration.setDefaultApiClient(client);
  }

  @Before
  public void before() throws Exception {
    CoreV1Api coreV1Api = new CoreV1Api();

    Map<String, String> valuesMapEu0 =
        new HashMap<String, String>() {
          {
            put(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME, "orientdb-eu0");
            put(StatefulSetTemplateKeys.ORIENTDB_LABEL, "orientdb");
            put(StatefulSetTemplateKeys.ORIENTDB_HTTP_PORT, "2480");
            put(StatefulSetTemplateKeys.ORIENTDB_BINARY_PORT, "2424");
            put(StatefulSetTemplateKeys.ORIENTDB_HAZELCAST_PORT, "2434");
            put(
                StatefulSetTemplateKeys.ORIENTDB_DOCKER_IMAGE,
                "192.168.2.119:5000/pxsalehi/orientdb:3.1.1");
            put(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM, "orientdb-eu0-cm");
            put(StatefulSetTemplateKeys.ORIENTDB_DB_VOL_SIZE, "2");
            put(StatefulSetTemplateKeys.ORIENTDB_HAZELCAST_CONFIG, "/kubernetes/hazelcast-0.xml");
            put(
                StatefulSetTemplateKeys.ORIENTDB_DISTRIBUTED_DB_CONFIG,
                "/kubernetes/default-distributed-db-config.json");
            put(
                StatefulSetTemplateKeys.ORIENTDB_SERVER_CONFIG,
                "/kubernetes/orientdb-simple-dserver-config-0.xml");
          }
        };

    Map<String, String> valuesMapEu1 =
        new HashMap<String, String>(valuesMapEu0) {
          {
            put(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME, "orientdb-eu1");
            put(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM, "orientdb-eu1-cm");
          }
        };
    Map<String, String> valuesMapEu2 =
        new HashMap<String, String>(valuesMapEu0) {
          {
            put(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME, "orientdb-eu2");
            put(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM, "orientdb-eu2-cm");
          }
        };
    deployOrientDB("default", valuesMapEu0);
    deployOrientDB("default", valuesMapEu1);
    deployOrientDB("default", valuesMapEu2);

    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    remote.close();
  }

  @Test
  public void sync()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
          InterruptedException {
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.createClass("One");
        session.save(session.newElement("One"));
        session.save(session.newElement("One"));
      }
      server2.shutdown();
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.save(session.newElement("One"));
      }
    }
    server0.shutdown();
    server1.shutdown();
    // Starting the servers in reverse shutdown order to trigger miss sync
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    // Test server 0
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 1
    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 2
    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
  }

  @Test
  public void reverseStartSync()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
          InterruptedException {
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.createClass("One");
        session.save(session.newElement("One"));
        session.save(session.newElement("One"));
      }
      server2.shutdown();
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.save(session.newElement("One"));
      }
    }
    server0.shutdown();
    server1.shutdown();
    // Starting the servers in reverse shutdown order to trigger miss sync
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    // Test server 0
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 1
    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 2
    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
  }

  @After
  public void after() throws InterruptedException {
    System.out.println("shutdown");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("test");
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }

  private String getEscapedFileContent(String fileName) throws URISyntaxException, IOException {
    String content = readAllLines(fileName);
    return content.replaceAll("\"", "\\\"");
  }

  private String readAllLines(String resourceFileName) throws URISyntaxException, IOException {
    List<String> lines =
        Files.readAllLines(Paths.get(getClass().getResource(resourceFileName).toURI()));
    return StringUtil.join(lines.toArray(new String[] {}), "\r\n");
  }

  private void deployOrientDB(String namespace, Map<String, String> params)
      throws IOException, URISyntaxException, ApiException {
    V1ConfigMap configMap =
        new V1ConfigMapBuilder()
            .withApiVersion("v1")
            .withKind("ConfigMap")
            .withNewMetadata()
            .withName(params.get(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM))
            .withNamespace(namespace)
            .endMetadata()
            .withData(
                new HashMap<String, String>() {
                  {
                    put(
                        "hazelcast.xml",
                        getEscapedFileContent(
                            params.get(StatefulSetTemplateKeys.ORIENTDB_HAZELCAST_CONFIG)));
                    put(
                        "default-distributed-db-config.json",
                        getEscapedFileContent(
                            params.get(StatefulSetTemplateKeys.ORIENTDB_DISTRIBUTED_DB_CONFIG)));
                    put(
                        "orientdb-server-config.xml",
                        getEscapedFileContent(
                            params.get(StatefulSetTemplateKeys.ORIENTDB_SERVER_CONFIG)));
                  }
                })
            .build();
    CoreV1Api coreV1Api = new CoreV1Api();
    coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
    StringSubstitutor substitutor = new StringSubstitutor(params);
    String statefulSetTemplate = readAllLines("/kubernetes/orientdb-statefulset-template.yaml");
    String statefulSetYamlStr = substitutor.replace(statefulSetTemplate);
    List<Object> objects = Yaml.loadAll(statefulSetYamlStr);
    if (!(objects.get(0) instanceof V1Service)) {
      fail("First object in template YAML must be a service.");
    }
    V1Service service = (V1Service) objects.get(0);

    if (!(objects.get(1) instanceof V1StatefulSet)) {
      fail("Second object in template YAML must be a statefulSet.");
    }
    V1StatefulSet statefulSet = (V1StatefulSet) objects.get(1);
    coreV1Api.createNamespacedService(namespace, service, null, null, null);
    AppsV1Api appsV1Api = new AppsV1Api();
    appsV1Api.createNamespacedStatefulSet(namespace, statefulSet, null, null, null);
    // must also remove PVCs
  }
}
