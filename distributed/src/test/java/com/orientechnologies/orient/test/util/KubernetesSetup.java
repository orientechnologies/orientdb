package com.orientechnologies.orient.test.util;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.StringUtil;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Yaml;
import okhttp3.OkHttpClient;
import org.apache.commons.text.StringSubstitutor;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    AppsV1Api appsV1Api = new AppsV1Api();
    try {
      // If server already exists, scale it up.
      String statefulSetName = params.get(TemplateKeys.ORIENTDB_NODE_NAME);
      V1StatefulSet statefulSet =
          appsV1Api.readNamespacedStatefulSet(statefulSetName, namespace, null, null, null);
      if (statefulSet != null) {
        scaleStatefulSet(statefulSetName, 1);
      } else {
        doStartServer(serverId, params);
      }
      waitForInstances(
          90,
          Collections.singletonList(serverId),
          String.format(
              "app=%s",
              params.get(TemplateKeys.ORIENTDB_LABEL))); // TODO: wait until server is ready
    } catch (ApiException e) {
      throw new OTestSetupException(e.getResponseBody(), e);
    } catch (IOException | URISyntaxException e) {
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
    String label =
        testConfig
            .getK8sConfigParams(testConfig.getServerIds().get(0))
            .get(TemplateKeys.ORIENTDB_LABEL);
    try {
      waitForInstances(90, testConfig.getServerIds(), String.format("app=%s", label));
    } catch (IOException e) {
      throw new OTestSetupException("Error waiting for server to start", e);
    } catch (ApiException e) {
      throw new OTestSetupException(e.getResponseBody(), e);
    }
  }

  private void waitForInstances(int timeoutSecond, List<String> serverIds, String labelSelector)
      throws ApiException, IOException {
    System.out.printf("Wait till instances %s are ready\n", serverIds);
    Set<String> ids = new HashSet<>(serverIds);

    ApiClient client = Configuration.getDefaultApiClient();
    // infinite timeout
    OkHttpClient httpClient =
        client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    client.setHttpClient(httpClient);
    Configuration.setDefaultApiClient(client);

    AppsV1Api appsV1Api = new AppsV1Api();
    Watch<V1StatefulSet> watch =
        Watch.createWatch(
            Configuration.getDefaultApiClient(),
            appsV1Api.listNamespacedStatefulSetCall(
                namespace,
                null,
                null,
                null,
                null,
                labelSelector, // TODO: does timeout work? w/ wrong label?
                null,
                null,
                timeoutSecond,
                true,
                null),
            new TypeToken<Watch.Response<V1StatefulSet>>() {}.getType());
    final long started = System.currentTimeMillis();
    try {
      // TODO: do we need another timer to get out of the watch loop?
      for (Watch.Response<V1StatefulSet> item : watch) {
        if (item.type.equalsIgnoreCase("ERROR")) {
          System.out.printf("Got error from watch: %s\n", item.status.getMessage());
        } else {
          String id = item.object.getMetadata().getName();
//          System.out.printf("Got watch update for %s, type=%s.\n", id, item.type);
          if (areThereReadyReplicas(item.object) && ids.contains(id)) {
            System.out.printf("  Server %s has at least one ready replica.\n", id);
            ids.remove(id);
            if (ids.isEmpty()) {
              System.out.printf("  All instances %s are ready.\n", serverIds);
              break;
            }
          }
//          else {
//            System.out.printf("Server %s is still not ready!\n", id);
//          }
        }
      }
    } finally {
//      System.out.println("Closing watch.");
      watch.close();
    }
    if (System.currentTimeMillis() > (started + timeoutSecond * 1000) && !ids.isEmpty()) {
      throw new OTestSetupException("Timed out waiting for instances to get ready.");
    }
  }

  private boolean areThereReadyReplicas(V1StatefulSet statefulSet) {
    if (statefulSet.getStatus() != null
        && statefulSet.getStatus().getReadyReplicas() != null
        && statefulSet.getStatus().getReadyReplicas() > 0) {
      return true;
    }
    return false;
  }

  @Override
  public void shutdownServer(String serverId) throws OTestSetupException {
    Map<String, String> param = testConfig.getK8sConfigParams(serverId);
    String name = param.get(TemplateKeys.ORIENTDB_NODE_NAME);
    try {
      scaleStatefulSet(name, 0);
    } catch (ApiException e) {
      throw new OTestSetupException(e.getResponseBody(), e);
    }
  }

  private void scaleStatefulSet(String statefulSetName, int newReplicaCount) throws ApiException {
    System.out.printf("Scaling %s to %d replicas.\n", statefulSetName, newReplicaCount);
    AppsV1Api appsV1Api = new AppsV1Api();
    V1Scale scale = appsV1Api.readNamespacedStatefulSetScale(statefulSetName, namespace, null);
    V1Scale newScale =
        new V1ScaleBuilder(scale).withNewSpec().withReplicas(newReplicaCount).endSpec().build();
    // Could also use patch
    appsV1Api.replaceNamespacedStatefulSetScale(
        statefulSetName, namespace, newScale, null, null, null);
  }

  @Override
  public void teardown() throws OTestSetupException {
    CoreV1Api coreV1Api = new CoreV1Api();
    AppsV1Api appsV1Api = new AppsV1Api();

    for (String serverId : testConfig.getServerIds()) {
      Map<String, String> nodeParam = testConfig.getK8sConfigParams(serverId);
      String configMapName = nodeParam.get(TemplateKeys.ORIENTDB_CONFIG_CM);
      String statefulSetName = nodeParam.get(TemplateKeys.ORIENTDB_NODE_NAME);
      try {
        coreV1Api.deleteNamespacedConfigMap(
            configMapName, "default", null, null, null, null, null, null);
        System.out.printf("deleted ConfigMap %s\n", configMapName);
      } catch (ApiException e) {
        System.out.printf("Error deleting ConfigMap %s: %s\n", configMapName, e.getResponseBody());
      }
      try {
        appsV1Api.deleteNamespacedStatefulSet(
            statefulSetName, "default", null, null, null, null, null, null);
        System.out.printf("deleted StatefulSet %s\n", statefulSetName);
      } catch (ApiException e) {
        System.out.printf(
            "Error deleting StatefulSet %s: %s\n", statefulSetName, e.getResponseBody());
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
        System.out.printf("Deleted Service %s\n", serviceName);
      } catch (ApiException e) {
        System.out.printf("Error deleting Service %s: %s\n", serviceName, e.getResponseBody());
      }
    }
    for (Iterator<String> it = PVCsToDelete.iterator(); it.hasNext(); ) {
      String pvc = it.next();
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
        it.remove();
      }
    }
  }

  @Override
  public String getAddress(String serverId, PortType port) {
    switch (port) {
    case HTTP:
      return testConfig.getK8sConfigParams(serverId).get(TemplateKeys.ORIENTDB_HTTP_ADDRESS);
    case BINARY:
      return testConfig.getK8sConfigParams(serverId).get(TemplateKeys.ORIENTDB_BINARY_ADDRESS);
    }
    return null;
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
