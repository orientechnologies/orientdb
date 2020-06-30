package com.orientechnologies.orient.test.util;

import com.google.gson.JsonSyntaxException;
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
import java.util.*;

public class KubernetesSetup implements TestSetup {
  public static final String ORIENTDB_HEADLESS_SERVICE_TEMPLATE =
      "/kubernetes/manifests/orientdb-headless-service.template.yaml";
  public static final String ORIENTDB_NODEPORT_SERVICE_TEMPLATE =
      "/kubernetes/manifests/orientdb-nodeport-service.template.yaml";
  public static final String ORIENTDB_STATEFULSET_TEMPLATE =
      "/kubernetes/manifests/orientdb-statefulset.template.yaml";

  private String nodeAddress;
  private TestConfig testConfig;
  // TODO: fetch namespace from env/mvn
  // TODO: should I use an execution profile? Exclude those not possible to setup on K8s.
  private String namespace = "default";
  private Set<String> PVCsToDelete = new HashSet<>();

  public KubernetesSetup(
      String kubeConfigFile, com.orientechnologies.orient.test.util.TestConfig config)
      throws IOException {
    this.testConfig = config;
    KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new FileReader(kubeConfigFile));
    String serverAddress = kubeConfig.getServer();
    nodeAddress = new URL(serverAddress).getHost();
    System.out.println("node address: " + nodeAddress);
    ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();
    Configuration.setDefaultApiClient(client);
  }

  @Override
  public void startServer(String serverId) throws OTestSetupException {
    Map<String, String> params = testConfig.getK8sConfigParams(serverId);
    if (params == null) {
      throw new OTestSetupException("Cannot get server parameters for " + serverId);
    }
    try {
      doStartServer(serverId, params);
      waitForInstances(90, null); // TODO: wait until server is ready
    } catch (ApiException e) {
      throw new OTestSetupException(e.getResponseBody(), e);
    } catch (IOException | URISyntaxException | InterruptedException e) {
      throw new OTestSetupException(e);
    }
  }

  @Override
  public void start() throws OTestSetupException {
    for (String serverId : testConfig.getServerIds()) {
      Map<String, String> params = testConfig.getK8sConfigParams(serverId);
      if (params == null) {
        throw new OTestSetupException("Cannot get server parameters for " + serverId);
      }
      try {
        doStartServer(serverId, params);
      } catch (ApiException e) {
        throw new OTestSetupException(e.getResponseBody(), e);
      } catch (IOException | URISyntaxException e) {
        throw new OTestSetupException(e);
      }
    }
    // TODO: wait until all servers are ready
    try {
      waitForInstances(90, null);
    } catch (InterruptedException e) {
      throw new OTestSetupException("Error waiting for server to start", e);
    }
  }

  private void waitForInstances(long timeoutSecond, List<String> serverIds)
      throws InterruptedException {
    System.out.println("wait till instance is ready");
    Thread.sleep(timeoutSecond * 1000);
    System.out.println("creating database...");
  }

  @Override
  public void shutdownServer(String serverId) throws OTestSetupException {
    AppsV1Api appsV1Api = new AppsV1Api();
    Map<String, String> param = testConfig.getK8sConfigParams(serverId);
    String name = param.get(TemplateKeys.ORIENTDB_NODE_NAME);
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
    } catch (ApiException e) {
      throw new OTestSetupException(e.getResponseBody(), e);
    }
  }

  private void scaleStatefulSet(String statefulSetName, int newReplicaCount) {}

  @Override
  public void teardown() throws OTestSetupException {
    CoreV1Api coreV1Api = new CoreV1Api();
    AppsV1Api appsV1Api = new AppsV1Api();

    for (String serverId : testConfig.getServerIds()) {
      Map<String, String> nodeParam = testConfig.getK8sConfigParams(serverId);
      String configMapName = nodeParam.get(TemplateKeys.ORIENTDB_CONFIG_CM);
      String nodeName = nodeParam.get(TemplateKeys.ORIENTDB_NODE_NAME);
      try {
        coreV1Api.deleteNamespacedConfigMap(
            configMapName, "default", null, null, null, null, null, null);
        System.out.printf("deleted ConfigMap %s\n", configMapName);
      } catch (ApiException e) {
        System.out.printf("Error deleting ConfigMap %s: %s\n", configMapName, e.getResponseBody());
      }
      try {
        appsV1Api.deleteNamespacedStatefulSet(
            nodeName, "default", null, null, null, null, null, null);
        System.out.printf("deleted StatefulSet %s\n", nodeName);
      } catch (ApiException e) {
        System.out.printf("Error deleting StatefulSet %s: %s\n", nodeName, e.getResponseBody());
      }
      String serviceName = nodeParam.get(TemplateKeys.ORIENTDB_NODE_NAME);
      try {
        coreV1Api.deleteNamespacedService(
            serviceName, "default", null, null, null, null, null, null);
        System.out.printf("deleted Service %s\n", serviceName);
      } catch (ApiException e) {
        System.out.printf("Error deleting Service %s: %s\n", serviceName, e.getResponseBody());
      }
      serviceName = nodeParam.get(TemplateKeys.ORIENTDB_NODE_NAME) + "-service";
      try {
        coreV1Api.deleteNamespacedService(
            serviceName, "default", null, null, null, null, null, null);
        System.out.printf("Deleted Service %s\n", nodeName);
      } catch (ApiException e) {
        System.out.printf("Error deleting Service %s: %s\n", serviceName, e.getResponseBody());
      }
    }
    for (String pvc : PVCsToDelete) {
      try {
        coreV1Api.deleteNamespacedPersistentVolumeClaim(
            pvc, "default", null, null, null, null, null, new V1DeleteOptions());
        System.out.printf("Deleted PVC %s\n", pvc);
      } catch (JsonSyntaxException e) {
        // There is a known bug with the auto-generated 'official' Kubernetes client for Java which
        // can lead to the following call throwing an exception, although it succeeds!
        // https://github.com/kubernetes-client/java/issues/86
      } catch (ApiException e) {
        System.out.printf("Error deleting PVC %s: %s\n", pvc, e.getResponseBody());
      } finally {
        // TODO: to work-around the bug, double-check that PVC is gone here!
        PVCsToDelete.remove(pvc);
      }
    }
  }

  @Override
  public String getRemoteAddress() {
    String serverId = testConfig.getServerIds().get(0);
    String address =
        testConfig.getK8sConfigParams(serverId).get(TemplateKeys.ORIENTDB_BINARY_ADDRESS);
    return "remote:" + address;
  }

  @Override
  public TestConfig getSetupConfig() {
    return testConfig;
  }

  private V1ConfigMap createConfigMap(String serverId, Map<String, String> serverParams)
      throws ApiException, IOException, URISyntaxException {
    CoreV1Api coreV1Api = new CoreV1Api();
    V1ConfigMap configMap =
        new V1ConfigMapBuilder()
            .withApiVersion("v1")
            .withKind("ConfigMap")
            .withNewMetadata()
            .withName(serverParams.get(TemplateKeys.ORIENTDB_CONFIG_CM))
            .withNamespace(namespace)
            .endMetadata()
            .withData(
                new HashMap<String, String>() {
                  {
                    put(
                        "hazelcast.xml",
                        getEscapedFileContent(
                            serverParams.get(TemplateKeys.ORIENTDB_HAZELCAST_CONFIG)));
                    put(
                        "default-distributed-db-config.json",
                        getEscapedFileContent(
                            serverParams.get(TemplateKeys.ORIENTDB_DISTRIBUTED_DB_CONFIG)));
                    put(
                        "orientdb-server-config.xml",
                        getEscapedFileContent(
                            serverParams.get(TemplateKeys.ORIENTDB_SERVER_CONFIG)));
                  }
                })
            .build();
    return coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
  }

  private V1Service createHeadlessService(String serverId, Map<String, String> serverParams)
      throws IOException, URISyntaxException, ApiException {
    return createService(serverId, serverParams, ORIENTDB_HEADLESS_SERVICE_TEMPLATE);
  }

  private V1Service createNodePortService(String serverId, Map<String, String> serverParams)
      throws IOException, URISyntaxException, ApiException {
    return createService(serverId, serverParams, ORIENTDB_NODEPORT_SERVICE_TEMPLATE);
  }

  private V1Service createService(
      String serverId, Map<String, String> serverParams, String templateFile)
      throws IOException, URISyntaxException, ApiException {
    CoreV1Api coreV1Api = new CoreV1Api();
    StringSubstitutor substitutor = new StringSubstitutor(serverParams);
    String template = readAllLines(templateFile);
    String manifest = substitutor.replace(template);
    V1Service service = (V1Service) Yaml.load(manifest);
    return coreV1Api.createNamespacedService(namespace, service, null, null, null);
  }

  private V1StatefulSet createStatefulSet(String serverId, Map<String, String> serverParams)
      throws IOException, URISyntaxException, ApiException {
    AppsV1Api appsV1Api = new AppsV1Api();
    StringSubstitutor substitutor = new StringSubstitutor(serverParams);
    String template = readAllLines(ORIENTDB_STATEFULSET_TEMPLATE);
    String manifest = substitutor.replace(template);
    V1StatefulSet statefulSet = (V1StatefulSet) Yaml.load(manifest);
    return appsV1Api.createNamespacedStatefulSet(namespace, statefulSet, null, null, null);
  }

  private void doStartServer(String serverId, Map<String, String> serverParams)
      throws ApiException, IOException, URISyntaxException {
    System.out.printf("Starting instance %s.\n", serverId);
    V1ConfigMap cm = createConfigMap(serverId, serverParams);
    System.out.printf("Created ConfigMap %s for %s.\n", cm.getMetadata().getName(), serverId);
    V1Service headless = createHeadlessService(serverId, serverParams);
    System.out.printf(
        "Created Headless Service %s for %s.\n", headless.getMetadata().getName(), serverId);
    V1StatefulSet statefulSet = createStatefulSet(serverId, serverParams);
    System.out.printf(
        "Created StatefulSet %s for %s.\n", statefulSet.getMetadata().getName(), serverId);
    // must also keep track of PVCs to remove later
    for (V1PersistentVolumeClaim pvc : statefulSet.getSpec().getVolumeClaimTemplates()) {
      PVCsToDelete.add(
          String.format(
              "%s-%s-0", pvc.getMetadata().getName(), statefulSet.getMetadata().getName()));
    }
    V1Service nodePort = createNodePortService(serverId, serverParams);
    System.out.printf(
        "Created NodePort Service %s for %s.\n", nodePort.getMetadata().getName(), serverId);
    nodePort
        .getSpec()
        .getPorts()
        .forEach(
            port -> {
              if (port.getName().equalsIgnoreCase("http")) {
                String httpAddress = String.format("%s:%d", nodeAddress, port.getNodePort());
                serverParams.put(TemplateKeys.ORIENTDB_HTTP_ADDRESS, httpAddress);
                System.out.printf("HTTP address for %s: %s\n", serverId, httpAddress);
              } else if (port.getName().equalsIgnoreCase("binary")) {
                String binaryAddress = String.format("%s:%d", nodeAddress, port.getNodePort());
                serverParams.put(TemplateKeys.ORIENTDB_BINARY_ADDRESS, binaryAddress);
                System.out.printf("Binary address for %s: %s\n", serverId, binaryAddress);
              }
            });
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
