package com.orientechnologies.orient.testutil;

import com.google.gson.JsonSyntaxException;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
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

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class TestSetup {
  public interface setup {
    void startServer(String serverId)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
            URISyntaxException, ApiException;

    void shutdownServer(String serverId) throws ApiException;

    void teardown() throws ApiException;

    String getRemoteAddress();

    TestConfigs.configs getSetupConfig();
  }

  public static class Local implements setup {
    private Map<String, OServer> servers = new HashMap<>();
    private TestConfigs.configs config;

    public Local(TestConfigs.configs config) {
      this.config = config;
    }

    @Override
    public void startServer(String serverId)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
      OServer server = servers.get(serverId);
      if (server == null) {
        server = OServer.startFromClasspathConfig(config.getLocalConfigFile(serverId));
        servers.put(serverId, server);
      } else {
        // TODO: is this necessary?
        if (!server.isActive()) {
          server.activate();
        }
      }
    }

    @Override
    public void shutdownServer(String serverId) {
      OServer server = servers.get(serverId);
      if (server != null) {
        server.shutdown();
      }
    }

    @Override
    public void teardown() {
      System.out.println("shutdown");
      OrientDB remote =
          new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
      remote.drop("test");
      remote.close();
      for (OServer server : servers.values()) server.shutdown();
      ODatabaseDocumentTx.closeAll();
    }

    @Override
    public String getRemoteAddress() {
      return "remote:localhost";
    }

    @Override
    public TestConfigs.configs getSetupConfig() {
      return config;
    }
  }

  public static class Kubernetes implements setup {
    private String nodeAddress;
    private TestConfigs.configs configs;
    private String namespace = "default";
    private List<String> PVCsToDelete = new ArrayList<>();

    public Kubernetes(String kubeConfigFile, TestConfigs.configs config) throws IOException {
      this.configs = config;
      KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new FileReader(kubeConfigFile));
      String serverAddress = kubeConfig.getServer();
      nodeAddress = new URL(serverAddress).getHost();
      System.out.println("node address: " + nodeAddress);
      ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();
      Configuration.setDefaultApiClient(client);
    }

    @Override
    public void startServer(String serverId) throws IOException, URISyntaxException, ApiException {
      Map<String, String> params = configs.getK8sConfigParams(serverId);
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
        // TODO: throw exception instead
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

      if (!(objects.get(2) instanceof V1Service)) {
        fail("Third object in template YAML must be a NodePort service.");
      }
      final V1Service nodePortService =
          coreV1Api.createNamespacedService(
              namespace, (V1Service) objects.get(2), null, null, null);
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
                  System.out.println(
                      StatefulSetTemplateKeys.ORIENTDB_HTTP_ADDRESS
                          + " is "
                          + params.get(StatefulSetTemplateKeys.ORIENTDB_HTTP_ADDRESS));
                } else if (port.getName().equalsIgnoreCase("binary")) {
                  params.put(
                      StatefulSetTemplateKeys.ORIENTDB_BINARY_ADDRESS,
                      String.format("%s:%d", nodeAddress, port.getNodePort()));
                }
              });
      // TODO: wait until server is ready
    }

    @Override
    public void shutdownServer(String serverId) throws ApiException {
      // TODO: also need to drop/close on remote?
      AppsV1Api appsV1Api = new AppsV1Api();
      Map<String, String> param = configs.getK8sConfigParams(serverId);
      String name = param.get(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME);
      try {
        System.out.println("fetching current scale...");
        V1Scale scale = appsV1Api.readNamespacedStatefulSetScale(name, "default", null);
        System.out.println("Got: " + scale);
        //    V1StatefulSet sset = appsV1Api.readNamespacedStatefulSet(name, "default", null, null,
        // null);
        V1Scale newScale =
            new V1ScaleBuilder(scale).withNewSpec().withReplicas(0).endSpec().build();
        System.out.println("setting new scale...");
        // Could also use patch
        appsV1Api.replaceNamespacedStatefulSetScale(name, "default", newScale, null, null, null);
      } catch (ApiException e) {
        System.out.println("Error scaling down: " + e.getResponseBody());
        throw e;
      }
    }

    @Override
    public void teardown() throws ApiException {
      CoreV1Api coreV1Api = new CoreV1Api();
      AppsV1Api appsV1Api = new AppsV1Api();

      for (String serverId : configs.getServerIds()) {
        Map<String, String> nodeParam = configs.getK8sConfigParams(serverId);
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

    @Override
    public String getRemoteAddress() {
      String serverId = configs.getServerIds().get(0);
      String address = configs.getK8sConfigParams(serverId).get(StatefulSetTemplateKeys.ORIENTDB_BINARY_ADDRESS);
      return "remote:" + address;
    }

    @Override
    public TestConfigs.configs getSetupConfig() {
      return configs;
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
  }

  public static setup createSetup(TestConfigs.configs configs) throws IOException {
    String kubeconfigFile = System.getProperty("kube.config");
    if (kubeconfigFile == null) {
      System.out.println("Running with local JVMs");
      return new Local(configs);
    }
    System.out.println("Running with Kube Config file " + kubeconfigFile);
    return new Kubernetes(kubeconfigFile, configs);
  }
}
