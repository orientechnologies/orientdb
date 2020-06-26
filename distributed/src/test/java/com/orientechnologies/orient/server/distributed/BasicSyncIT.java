package com.orientechnologies.orient.server.distributed;

import com.google.gson.JsonSyntaxException;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.testutil.NodePortForward;
import com.orientechnologies.orient.testutil.StatefulSetTemplateKeys;
import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.StringUtil;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BasicSyncIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;
  private List<String> PVCsToDelete = new ArrayList<>();
  private List<Map<String, String>> nodeParams = new ArrayList<>();
  private static String nodeAddress;

  @BeforeClass
  public static void setupKubernetesClient() throws IOException {
    String kubeConfigFilePath = "/home/pxsalehi/.kube/config";
    KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new FileReader(kubeConfigFilePath));
    String serverAddress = kubeConfig.getServer();
    nodeAddress = new URL(serverAddress).getHost();
    System.out.println("node address: " + nodeAddress);
    ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();
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
    try {
      deployOrientDB("default", valuesMapEu0);
      nodeParams.add(valuesMapEu0);
      deployOrientDB("default", valuesMapEu1);
      nodeParams.add(valuesMapEu1);
      deployOrientDB("default", valuesMapEu2);
      nodeParams.add(valuesMapEu2);
    } catch (ApiException e) {
      System.out.println("Error while deploying instances: " + e.getMessage());
      System.out.println(e.getResponseBody());
      fail();
    }

//    NodePortForward.create("default", String.format("%s-0",
//        valuesMapEu0.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME)), 2424, 2424);

//    int localPort = 2424, remotePort = 2424;
//    PortForward forward = new PortForward();
//    List<Integer> ports = new ArrayList<>();
//    ports.add(localPort);
//    ports.add(remotePort);
//    final PortForward.PortForwardResult result = forward.forward("default", String.format("%s-0", valuesMapEu0.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME)), ports);
//
//    ServerSocket ss = new ServerSocket(localPort);
//
//    final Socket s = ss.accept();
//    System.out.println("Connected!");

    System.out.println("wait till instance is ready");
    Thread.sleep(90 * 1000);
    System.out.println("creating database...");

//    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
//    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
//    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
//    OrientDB remote =
//        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());

    OrientDB remote =
        new OrientDB("remote:" + valuesMapEu0.get(StatefulSetTemplateKeys.ORIENTDB_BINARY_ADDRESS), "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    remote.close();
    System.out.println("created database 'test'");
  }

  private void shutdownInstance(Map<String, String> param) throws ApiException {
    AppsV1Api appsV1Api = new AppsV1Api();
    String name = param.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME);
    try {
      System.out.println("fetching current scale...");
      V1Scale scale = appsV1Api.readNamespacedStatefulSetScale(name, "default", null);
      System.out.println("Got: " + scale);
      //    V1StatefulSet sset = appsV1Api.readNamespacedStatefulSet(name, "default", null, null,
      // null);
      V1Scale newScale = new V1ScaleBuilder(scale).withNewSpec().withReplicas(0).endSpec().build();
      System.out.println("setting new scale...");
      // Could also use patch
      appsV1Api.replaceNamespacedStatefulSetScale(name, "default", newScale, null, null, null);
    } catch(ApiException e) {
      System.out.println("Error scaling down: " + e.getResponseBody());
      throw e;
    }
  }

  @Test
  public void sync()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, InterruptedException,
      ApiException {
//    String remoteAddress = "remote:localhost";
    String remoteAddress = "remote:" + nodeParams.get(0).get(StatefulSetTemplateKeys.ORIENTDB_BINARY_ADDRESS);
//    try (OrientDB remote = new OrientDB(remoteAddress, OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.createClass("One");
//        session.save(session.newElement("One"));
//        session.save(session.newElement("One"));
//      }
//      server2.shutdown();
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.save(session.newElement("One"));
//      }
//      System.out.println("saved session.");
//    }
    System.out.println("taking down instance 2");
    shutdownInstance(nodeParams.get(1));
    System.out.println("waiting for scale down to settle!");
    Thread.sleep(90 * 1000);
//    server0.shutdown();
//    server1.shutdown();
//    // Starting the servers in reverse shutdown order to trigger miss sync
//    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
//    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
//    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
//    // Test server 0
//    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 1
//    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 2
//    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
  }

//  @Test
//  public void reverseStartSync()
//      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
//          InterruptedException {
//    //    System.out.println("waiting...");
//    //    Thread.sleep(5 * 60 * 1000);
//
//    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.createClass("One");
//        session.save(session.newElement("One"));
//        session.save(session.newElement("One"));
//      }
//      server2.shutdown();
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.save(session.newElement("One"));
//      }
//    }
//    server0.shutdown();
//    server1.shutdown();
//    // Starting the servers in reverse shutdown order to trigger miss sync
//    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
//    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
//    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
//    // Test server 0
//    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 1
//    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 2
//    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//  }

  @After
  public void after() throws InterruptedException, ApiException {
//    System.out.println("shutdown");
//    OrientDB remote =
//        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
//    remote.drop("test");
//    remote.close();
//
//    server0.shutdown();
//    server1.shutdown();
//    server2.shutdown();
//    ODatabaseDocumentTx.closeAll();

    CoreV1Api coreV1Api = new CoreV1Api();
    AppsV1Api appsV1Api = new AppsV1Api();
    for (Map<String, String> nodeParam : nodeParams) {
      String configMapName = nodeParam.get(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM);
      String nodeName = nodeParam.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME);
      coreV1Api.deleteNamespacedConfigMap(
          configMapName, "default", null, null, null, null, null, null);
      System.out.printf("deleted ConfigMap %s\n", configMapName);
      appsV1Api.deleteNamespacedStatefulSet(
          nodeName, "default", null, null, null, null, null, null);
      System.out.printf("deleted StatefulSet %s\n", nodeName);
      coreV1Api.deleteNamespacedService(
          nodeParam.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME),
          "default",
          null,
          null,
          null,
          null,
          null,
          null);
      System.out.printf("deleted Service %s\n", nodeName);
      coreV1Api.deleteNamespacedService(
          nodeParam.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME) + "-service",
          "default",
          null,
          null,
          null,
          null,
          null,
          null);
      System.out.printf("deleted Node Port Service %s\n", nodeName);
    }
    for (String pvc : PVCsToDelete) {
      // There is a known bug with the auto-generated 'official' Kubernetes client for Java which
      // can lead to
      // the following call throwing an exception, although it succeeds!
      // https://github.com/kubernetes-client/java/issues/86
      try {
        coreV1Api.deleteNamespacedPersistentVolumeClaim(
            pvc, "default", null, null, null, null, null, new V1DeleteOptions());
      } catch (JsonSyntaxException e) {
        System.out.println("Error while deleting PVC: " + e.getMessage());
      }
      System.out.printf("deleted PVC %s\n", pvc);
    }
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
    System.out.printf("created ConfigMap %s\n", configMap.getMetadata().getName());
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
    System.out.printf("created Service %s\n", statefulSet.getMetadata().getName());
    AppsV1Api appsV1Api = new AppsV1Api();
    appsV1Api.createNamespacedStatefulSet(namespace, statefulSet, null, null, null);
    System.out.printf("created StatefulSet %s\n", statefulSet.getMetadata().getName());
    // must also remove PVCs
    for (V1PersistentVolumeClaim pvc : statefulSet.getSpec().getVolumeClaimTemplates()) {
      PVCsToDelete.add(
          String.format(
              "%s-%s-0", pvc.getMetadata().getName(), statefulSet.getMetadata().getName()));
    }

    if(!(objects.get(2) instanceof V1Service)) {
      fail("Third object in template YAML must be a NodePort service.");
    }
    final V1Service nodePortService = coreV1Api.createNamespacedService(namespace, (V1Service) objects.get(2), null, null, null);
    System.out.printf("created node port Service %s\n", nodePortService.getMetadata().getName());
    System.out.println("base path: " + Configuration.getDefaultApiClient().getBasePath());
    nodePortService
        .getSpec()
        .getPorts()
        .forEach(
            port -> {
              System.out.printf("  %s=%d\n", port.getName(), port.getNodePort());
              if (port.getName().equalsIgnoreCase("http")) {
                params.put(
                    StatefulSetTemplateKeys.ORIENTDB_HTTP_ADDRESS,
                    String.format("%s:%d", nodeAddress, port.getNodePort()));
                System.out.println(StatefulSetTemplateKeys.ORIENTDB_HTTP_ADDRESS + " is " + params.get(StatefulSetTemplateKeys.ORIENTDB_HTTP_ADDRESS));
              } else if (port.getName().equalsIgnoreCase("binary")) {
                params.put(
                    StatefulSetTemplateKeys.ORIENTDB_BINARY_ADDRESS,
                    String.format("%s:%d", nodeAddress, port.getNodePort()));
              }
            });
  }
}
