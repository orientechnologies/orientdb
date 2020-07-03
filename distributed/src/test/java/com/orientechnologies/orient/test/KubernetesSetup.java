package com.orientechnologies.orient.test;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Yaml;
import okhttp3.OkHttpClient;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KubernetesSetup implements TestSetup {
  // Used for listing OrientDB stateful sets.
  private static final String statefulSetLabelSelector =
      String.format("app=%s", TestSetupUtil.getOrientDBKubernetesLabel());
  private static final int readyReplicaTimeoutSeconds = 90;

  private String nodeAddress;
  private TestConfig testConfig;
  // TODO: fetch namespace from env/mvn
  private String namespace = "default";
  private Set<String> PVCsToDelete = new HashSet<>();

  public KubernetesSetup(String kubeConfigFile, TestConfig config) throws IOException {
    this.testConfig = config;
    KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new FileReader(kubeConfigFile));
    String serverAddress = kubeConfig.getServer();
    nodeAddress = new URL(serverAddress).getHost();
    System.out.println("node address: " + nodeAddress);
    ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();
    Configuration.setDefaultApiClient(client);
  }

  @Override
  public void startServer(String serverId) throws TestSetupException {
    K8sServerConfig serverConfig = testConfig.getK8sConfigs(serverId);
    serverConfig.validate();
    AppsV1Api appsV1Api = new AppsV1Api();
    try {
      // If server already exists, scale it up.
      String statefulSetName = serverConfig.getNodeName();
      V1StatefulSet statefulSet =
          appsV1Api.readNamespacedStatefulSet(statefulSetName, namespace, null, null, null);
      if (statefulSet != null) {
        scaleStatefulSet(statefulSetName, 1);
      } else {
        doStartServer(serverId, serverConfig);
      }
      waitForInstances(
          readyReplicaTimeoutSeconds,
          Collections.singletonList(serverId),
          statefulSetLabelSelector);
    } catch (ApiException e) {
      throw new TestSetupException(e.getResponseBody(), e);
    } catch (IOException | URISyntaxException e) {
      throw new TestSetupException(e);
    }
  }

  @Override
  public void start() throws TestSetupException {
    for (String serverId : testConfig.getServerIds()) {
      K8sServerConfig serverConfig = testConfig.getK8sConfigs(serverId);
      serverConfig.validate();
      try {
        doStartServer(serverId, serverConfig);
      } catch (ApiException e) {
        throw new TestSetupException(e.getResponseBody(), e);
      } catch (IOException | URISyntaxException e) {
        throw new TestSetupException(e);
      }
    }

    try {
      waitForInstances(
          readyReplicaTimeoutSeconds, testConfig.getServerIds(), statefulSetLabelSelector);
    } catch (IOException e) {
      throw new TestSetupException("Error waiting for server to start", e);
    } catch (ApiException e) {
      throw new TestSetupException(e.getResponseBody(), e);
    }
  }

  private void waitForInstances(int timeoutSecond, List<String> serverIds, String labelSelector)
      throws ApiException, IOException {
    System.out.printf("Wait till instances %s are ready\n", serverIds);
    Set<String> ids = new HashSet<>(serverIds);

    ApiClient client = Configuration.getDefaultApiClient();
    OkHttpClient httpClient =
        client.getHttpClient().newBuilder().readTimeout(300, TimeUnit.SECONDS).build();
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
                labelSelector,
                null,
                null,
                timeoutSecond,
                true,
                null),
            new TypeToken<Watch.Response<V1StatefulSet>>() {}.getType());
    final long started = System.currentTimeMillis();
    try {
      for (Watch.Response<V1StatefulSet> item : watch) {
        if (item.type.equalsIgnoreCase("ERROR")) {
          System.out.printf("Got error from watch: %s\n", item.status.getMessage());
        } else {
          String id = item.object.getMetadata().getName();
          if (areThereReadyReplicas(item.object) && ids.contains(id)) {
            System.out.printf("  Server %s has at least one ready replica.\n", id);
            ids.remove(id);
            if (ids.isEmpty()) {
              System.out.printf("  All instances %s are ready.\n", serverIds);
              break;
            }
          }
        }
      }
    } finally {
      watch.close();
    }
    if (System.currentTimeMillis() > (started + timeoutSecond * 1000) && !ids.isEmpty()) {
      throw new TestSetupException("Timed out waiting for instances to get ready.");
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
  public void shutdownServer(String serverId) throws TestSetupException {
    K8sServerConfig config = testConfig.getK8sConfigs(serverId);
    String name = config.getNodeName();
    try {
      scaleStatefulSet(name, 0);
    } catch (ApiException e) {
      throw new TestSetupException(e.getResponseBody(), e);
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
  public void teardown() throws TestSetupException {
    CoreV1Api coreV1Api = new CoreV1Api();
    AppsV1Api appsV1Api = new AppsV1Api();

    for (String serverId : testConfig.getServerIds()) {
      System.out.printf("Tearing down node %s.\n", serverId);
      K8sServerConfig config = testConfig.getK8sConfigs(serverId);
      String configMapName = config.getConfigMapName();
      String statefulSetName = config.getNodeName();
      try {
        coreV1Api.deleteNamespacedConfigMap(
            configMapName, "default", null, null, null, null, null, null);
        System.out.printf("  Deleted ConfigMap %s\n", configMapName);
      } catch (ApiException e) {
        System.out.printf("  Error deleting ConfigMap %s: %s\n", configMapName, e.getResponseBody());
      }
      try {
        appsV1Api.deleteNamespacedStatefulSet(
            statefulSetName, "default", null, null, null, null, null, null);
        System.out.printf("  Deleted StatefulSet %s\n", statefulSetName);
      } catch (ApiException e) {
        System.out.printf(
            "  Error deleting StatefulSet %s: %s\n", statefulSetName, e.getResponseBody());
      }
      String serviceName = config.getNodeName();
      try {
        coreV1Api.deleteNamespacedService(
            serviceName, "default", null, null, null, null, null, null);
        System.out.printf("  Deleted Service %s\n", serviceName);
      } catch (ApiException e) {
        System.out.printf("  Error deleting Service %s: %s\n", serviceName, e.getResponseBody());
      }
      serviceName = config.getNodeName() + "-service";
      try {
        coreV1Api.deleteNamespacedService(
            serviceName, "default", null, null, null, null, null, null);
        System.out.printf("  Deleted Service %s\n", serviceName);
      } catch (ApiException e) {
        System.out.printf("  Error deleting Service %s: %s\n", serviceName, e.getResponseBody());
      }
    }
    System.out.println("Removing PVCs.");
    for (Iterator<String> it = PVCsToDelete.iterator(); it.hasNext(); ) {
      String pvc = it.next();
      try {
        coreV1Api.deleteNamespacedPersistentVolumeClaim(
            pvc, "default", null, null, null, null, null, new V1DeleteOptions());
        System.out.printf("  Deleted PVC %s\n", pvc);
      } catch (JsonSyntaxException e) {
        // There is a known bug with the auto-generated 'official' Kubernetes client for Java which
        // can lead to the following call throwing an exception, although it succeeds!
        // https://github.com/kubernetes-client/java/issues/86
      } catch (ApiException e) {
        System.out.printf("  Error deleting PVC %s: %s\n", pvc, e.getResponseBody());
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
        return testConfig.getK8sConfigs(serverId).getHttpAddress();
      case BINARY:
        return testConfig.getK8sConfigs(serverId).getBinaryAddress();
    }
    return null;
  }

  @Override
  public OrientDB createRemote(String serverId, OrientDBConfig config) {
    return createRemote(serverId, null, null, config);
  }

  @Override
  public OrientDB createRemote(
      String serverId, String serverUser, String serverPassword, OrientDBConfig config) {
    return new OrientDBIT(
        "remote:" + getAddress(serverId, PortType.BINARY), serverUser, serverPassword, config);
  }

  private V1ConfigMap createConfigMap(String serverId, K8sServerConfig config)
      throws ApiException, IOException, URISyntaxException {
    CoreV1Api coreV1Api = new CoreV1Api();
    V1ConfigMap configMap =
        new V1ConfigMapBuilder()
            .withApiVersion("v1")
            .withKind("ConfigMap")
            .withNewMetadata()
            .withName(config.getConfigMapName())
            .withNamespace(namespace)
            .endMetadata()
            .withData(
                new HashMap<String, String>() {
                  {
                    put("hazelcast.xml", getEscapedFileContent(config.getHazelcastConfig()));
                    put(
                        "default-distributed-db-config.json",
                        getEscapedFileContent(config.getDistributedDBConfig()));
                    put(
                        "orientdb-server-config.xml",
                        getEscapedFileContent(config.getServerConfig()));
                  }
                })
            .build();
    return coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
  }

  private V1Service createHeadlessService(String serverId, K8sServerConfig config)
      throws IOException, URISyntaxException, ApiException {
    CoreV1Api coreV1Api = new CoreV1Api();
    String manifest = ManifestTemplate.generateHeadlessService(config);
    V1Service service = (V1Service) Yaml.load(manifest);
    return coreV1Api.createNamespacedService(namespace, service, null, null, null);
  }

  private V1Service createNodePortService(String serverId, K8sServerConfig config)
      throws IOException, URISyntaxException, ApiException {
    CoreV1Api coreV1Api = new CoreV1Api();
    String manifest = ManifestTemplate.generateNodePortService(config);
    V1Service service = (V1Service) Yaml.load(manifest);
    return coreV1Api.createNamespacedService(namespace, service, null, null, null);
  }

  private V1StatefulSet createStatefulSet(String serverId, K8sServerConfig config)
      throws IOException, URISyntaxException, ApiException {
    AppsV1Api appsV1Api = new AppsV1Api();
    String manifest = ManifestTemplate.generateStatefulSet(config);
    V1StatefulSet statefulSet = (V1StatefulSet) Yaml.load(manifest);
    return appsV1Api.createNamespacedStatefulSet(namespace, statefulSet, null, null, null);
  }

  private void doStartServer(String serverId, K8sServerConfig config)
      throws ApiException, IOException, URISyntaxException {
    System.out.printf("Starting instance %s.\n", serverId);
    V1ConfigMap cm = createConfigMap(serverId, config);
    System.out.printf("  Created ConfigMap %s for %s.\n", cm.getMetadata().getName(), serverId);
    V1Service headless = createHeadlessService(serverId, config);
    System.out.printf(
        "  Created Headless Service %s for %s.\n", headless.getMetadata().getName(), serverId);
    V1StatefulSet statefulSet = createStatefulSet(serverId, config);
    System.out.printf(
        "  Created StatefulSet %s for %s.\n", statefulSet.getMetadata().getName(), serverId);
    // must also keep track of PVCs to remove later
    for (V1PersistentVolumeClaim pvc : statefulSet.getSpec().getVolumeClaimTemplates()) {
      PVCsToDelete.add(
          String.format(
              "%s-%s-0", pvc.getMetadata().getName(), statefulSet.getMetadata().getName()));
    }
    V1Service nodePort = createNodePortService(serverId, config);
    System.out.printf(
        "  Created NodePort Service %s for %s.\n", nodePort.getMetadata().getName(), serverId);
    nodePort
        .getSpec()
        .getPorts()
        .forEach(
            port -> {
              if (port.getName().equalsIgnoreCase("http")) {
                String httpAddress = String.format("%s:%d", nodeAddress, port.getNodePort());
                config.setHttpAddress(httpAddress);
                System.out.printf("  HTTP address for %s: %s\n", serverId, httpAddress);
              } else if (port.getName().equalsIgnoreCase("binary")) {
                String binaryAddress = String.format("%s:%d", nodeAddress, port.getNodePort());
                config.setBinaryAddress(binaryAddress);
                System.out.printf("  Binary address for %s: %s\n", serverId, binaryAddress);
              }
            });
  }

  public static String getEscapedFileContent(String fileName)
      throws URISyntaxException, IOException {
    String content = TestSetupUtil.readAllLines(fileName);
    return content.replaceAll("\"", "\\\"");
  }
}
