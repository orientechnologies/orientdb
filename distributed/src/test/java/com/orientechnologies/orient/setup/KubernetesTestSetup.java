package com.orientechnologies.orient.setup;

import static com.orientechnologies.orient.setup.TestSetupUtil.log;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Yaml;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;

public class KubernetesTestSetup implements TestSetup {
  // Used for listing OrientDB stateful sets.
  private static final String statefulSetLabelSelector =
      String.format("app=%s", TestSetupUtil.getOrientDBKubernetesLabel());
  private static final int readyReplicaTimeoutSeconds = 5 * 60;

  private SetupConfig setupConfig;
  // The namespace to setup the cluster and run tests. It must already exist.
  private String namespace = TestSetupUtil.getKubernetesNamespace();
  private Set<String> PVCsToDelete = new HashSet<>();
  // Port forwarders per server
  private Map<String, List<PortForwarder>> portforwarders = new HashMap<>();

  public KubernetesTestSetup(String kubeConfigFile, SetupConfig config) throws TestSetupException {
    try {
      this.setupConfig = config;
      KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new FileReader(kubeConfigFile));
      ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();
      Configuration.setDefaultApiClient(client);
    } catch (IOException e) {
      e.printStackTrace();
      throw new TestSetupException(e);
    }
  }

  public static String getEscapedFileContent(String fileName)
      throws URISyntaxException, IOException {
    String content = TestSetupUtil.readAllLines(fileName);
    return content.replaceAll("\"", "\\\"");
  }

  /**
   * Can be called during the test to scale up a server that has been previously scaled to 0
   * (shutdown).
   *
   * @param serverId
   * @throws TestSetupException
   */
  @Override
  public void startServer(String serverId) throws TestSetupException {
    K8sServerConfig serverConfig = setupConfig.getK8sConfigs(serverId);
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
      setupPortForward(serverId, serverConfig);
    } catch (ApiException e) {
      throw new TestSetupException(e.getResponseBody(), e);
    } catch (IOException | URISyntaxException e) {
      throw new TestSetupException(e);
    }
  }

  /**
   * Should be called only once before the test starts. Assumes none of the servers already exists
   * and starts them in parallel.
   *
   * @throws TestSetupException
   */
  @Override
  public void setup() throws TestSetupException {
    try {
      createRBAC();
      log("Starting servers...");
      for (String serverId : setupConfig.getServerIds()) {
        K8sServerConfig serverConfig = setupConfig.getK8sConfigs(serverId);
        serverConfig.validate();
        doStartServer(serverId, serverConfig);
      }
      waitForInstances(
          readyReplicaTimeoutSeconds, setupConfig.getServerIds(), statefulSetLabelSelector);
      for (String serverId : setupConfig.getServerIds()) {
        setupPortForward(serverId, setupConfig.getK8sConfigs(serverId));
      }
    } catch (ApiException e) {
      e.printStackTrace();
      throw new TestSetupException(e.getResponseBody(), e);
    } catch (IOException | URISyntaxException e) {
      e.printStackTrace();
      throw new TestSetupException(e);
    }
  }

  private void waitForInstances(
      int timeoutSecond, Collection<String> serverIds, String labelSelector)
      throws ApiException, IOException {
    log("Wait till instances %s are ready.", serverIds);
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
          log("  Got error from watch: %s", item.status.getMessage());
        } else {
          String id = item.object.getMetadata().getName();
          if (areThereReadyReplicas(item.object) && ids.contains(id)) {
            log("  Server %s has at least one ready replica.", id);
            ids.remove(id);
            if (ids.isEmpty()) {
              log("  All instances %s are ready.", serverIds);
              break;
            }
          }
        }
      }
    } finally {
      watch.close();
    }
    if (System.currentTimeMillis() > (started + timeoutSecond * 1000) && !ids.isEmpty()) {
      throw new TestSetupException("  Timed out waiting for instances to get ready.");
    }
  }

  private boolean areThereReadyReplicas(V1StatefulSet statefulSet) {
    if (statefulSet.getStatus() != null
        && statefulSet.getStatus().getReadyReplicas() != null
        && statefulSet.getStatus().getReadyReplicas() > 0
        && statefulSet.getStatus().getCurrentReplicas() != null
        && statefulSet.getStatus().getCurrentReplicas() > 0) {
      return true;
    }
    return false;
  }

  @Override
  public void shutdownServer(String serverId) throws TestSetupException {
    K8sServerConfig config = setupConfig.getK8sConfigs(serverId);
    String name = config.getNodeName();
    try {
      stopPortForward(serverId);
      scaleStatefulSet(name, 0);
    } catch (ApiException e) {
      throw new TestSetupException(e.getResponseBody(), e);
    }
  }

  private void scaleStatefulSet(String statefulSetName, int newReplicaCount) throws ApiException {
    log("Scaling %s to %d replicas.", statefulSetName, newReplicaCount);
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

    for (String serverId : setupConfig.getServerIds()) {
      log("Stop port-forward for node %s.", serverId);
      stopPortForward(serverId);
      log("Tearing down node %s.", serverId);
      K8sServerConfig config = setupConfig.getK8sConfigs(serverId);
      String configMapName = config.getConfigMapName();
      String statefulSetName = config.getNodeName();
      try {
        coreV1Api.deleteNamespacedConfigMap(
            configMapName, namespace, null, null, null, null, null, null);
        log("  Deleted ConfigMap %s", configMapName);
      } catch (ApiException e) {
        log("  Error deleting ConfigMap %s: %s", configMapName, e.getResponseBody());
      }
      try {
        appsV1Api.deleteNamespacedStatefulSet(
            statefulSetName, namespace, null, null, null, null, null, null);
        log("  Deleted StatefulSet %s", statefulSetName);
      } catch (ApiException e) {
        log("  Error deleting StatefulSet %s: %s", statefulSetName, e.getResponseBody());
      }
      String serviceName = config.getNodeName();
      try {
        coreV1Api.deleteNamespacedService(
            serviceName, namespace, null, null, null, null, null, null);
        log("  Deleted Service %s", serviceName);
      } catch (ApiException e) {
        log("  Error deleting Service %s: %s", serviceName, e.getResponseBody());
      }
    }
    System.out.println("Removing PVCs.");
    for (Iterator<String> it = PVCsToDelete.iterator(); it.hasNext(); ) {
      String pvc = it.next();
      try {
        coreV1Api.deleteNamespacedPersistentVolumeClaim(
            pvc, namespace, null, null, null, null, null, new V1DeleteOptions());
        log("  Deleted PVC %s", pvc);
      } catch (JsonSyntaxException e) {
        // There is a known bug with the auto-generated 'official' Kubernetes client for Java which
        // can lead to the following call throwing an exception, although it succeeds!
        // https://github.com/kubernetes-client/java/issues/86
      } catch (ApiException e) {
        log("  Error deleting PVC %s: %s", pvc, e.getResponseBody());
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
        return setupConfig.getK8sConfigs(serverId).getHttpAddress();
      case BINARY:
        return setupConfig.getK8sConfigs(serverId).getBinaryAddress();
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
    return createRemote(Collections.singleton(serverId), serverUser, serverPassword, config);
  }

  @Override
  public OrientDB createRemote(
      Collection<String> serverIds,
      String serverUser,
      String serverPassword,
      OrientDBConfig config) {
    Optional<String> url =
        serverIds.stream()
            .map(id -> "remote:" + getAddress(id, PortType.BINARY))
            .reduce((s1, s2) -> s1 + ";" + s2);
    log("Creating remote connection to server(s) '%s' at '%s'.", serverIds, url.get());
    return new OrientDBIT(url.orElse(""), serverUser, serverPassword, config);
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
                    put(
                        "orientdb-server-log.properties",
                        getEscapedFileContent(config.getServerLogConfig()));
                    put(
                        "orientdb-client-log.properties",
                        getEscapedFileContent(config.getClientLogConfig()));
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

  private V1StatefulSet createStatefulSet(String serverId, K8sServerConfig config)
      throws IOException, URISyntaxException, ApiException {
    AppsV1Api appsV1Api = new AppsV1Api();
    String manifest = ManifestTemplate.generateStatefulSet(config);
    V1StatefulSet statefulSet = (V1StatefulSet) Yaml.load(manifest);
    return appsV1Api.createNamespacedStatefulSet(namespace, statefulSet, null, null, null);
  }

  private void createRBAC() throws IOException, URISyntaxException, ApiException {
    log("Creating  RBAC.");
    CoreV1Api coreV1Api = new CoreV1Api();
    RbacAuthorizationV1Api rbacV1Api = new RbacAuthorizationV1Api();
    String manifests = ManifestTemplate.generateRBAC();
    for (Object obj : Yaml.loadAll(manifests)) {
      if (obj instanceof V1ServiceAccount) {
        V1ServiceAccount sa = (V1ServiceAccount) obj;
        String name = sa.getMetadata().getName();
        if (serviceAccountExists(coreV1Api, namespace, name)) {
          log("  Service account '%s' already exists.", name);
        } else {
          coreV1Api.createNamespacedServiceAccount(namespace, sa, null, null, null);
        }
      } else if (obj instanceof V1Role) {
        V1Role r = (V1Role) obj;
        rbacV1Api.replaceNamespacedRole(
            r.getMetadata().getName(), namespace, (V1Role) obj, null, null, null);
      } else if (obj instanceof V1RoleBinding) {
        V1RoleBinding rb = (V1RoleBinding) obj;
        rbacV1Api.replaceNamespacedRoleBinding(
            rb.getMetadata().getName(), namespace, (V1RoleBinding) obj, null, null, null);
      } else {
        log("Ignoring Kubernetes object %s when creating RBAC.", obj.getClass().getSimpleName());
      }
    }
  }

  private boolean serviceAccountExists(CoreV1Api api, String namespace, String name) {
    try {
      if (api.readNamespacedServiceAccount(name, namespace, null, null, null) != null) {
        return true;
      }
    } catch (ApiException e) {
    }
    return false;
  }

  private void doStartServer(String serverId, K8sServerConfig config)
      throws ApiException, IOException, URISyntaxException {
    log("Starting instance %s.", serverId);
    V1ConfigMap cm = createConfigMap(serverId, config);
    log("  Created ConfigMap %s for %s.", cm.getMetadata().getName(), serverId);
    V1Service headless = createHeadlessService(serverId, config);
    log("  Created Headless Service %s for %s.", headless.getMetadata().getName(), serverId);
    V1StatefulSet statefulSet = createStatefulSet(serverId, config);
    log("  Created StatefulSet %s for %s.", statefulSet.getMetadata().getName(), serverId);
    // must also keep track of PVCs to remove later
    for (V1PersistentVolumeClaim pvc : statefulSet.getSpec().getVolumeClaimTemplates()) {
      PVCsToDelete.add(
          String.format(
              "%s-%s-0", pvc.getMetadata().getName(), statefulSet.getMetadata().getName()));
    }
  }

  private void setupPortForward(String serverId, K8sServerConfig config)
      throws IOException, ApiException {
    log("Setting up port-forward for %s.", serverId);
    // Each server has its own StatefulSet with its name and has one replica. Therefore, pod name is
    // always the same.
    String serverPod = String.format("%s-0", serverId);
    PortForwarder binaryPortforward =
        new PortForwarder(
            namespace,
            serverPod,
            Integer.parseInt(config.getBinaryPort()),
            true,
            config.getServerUser(),
            config.getServerPass());
    PortForwarder httpPortforward =
        new PortForwarder(
            namespace,
            serverPod,
            Integer.parseInt(config.getHttpPort()),
            false,
            config.getServerUser(),
            config.getServerPass());

    int localBinaryPort = binaryPortforward.start();
    String binaryAddress = String.format("localhost:%d", localBinaryPort);
    config.setBinaryAddress(binaryAddress);
    log("  Binary address for %s: %s", serverId, binaryAddress);

    int localHttpPort = httpPortforward.start();
    String httpAddress = String.format("localhost:%d", localHttpPort);
    config.setHttpAddress(httpAddress);
    log("  HTTP address for %s: %s", serverId, httpAddress);

    List<PortForwarder> forwarders = new LinkedList<>();
    forwarders.add(binaryPortforward);
    forwarders.add(httpPortforward);
    portforwarders.put(serverId, forwarders);
  }

  private void stopPortForward(String serverId) {
    List<PortForwarder> pfs = portforwarders.get(serverId);
    if (pfs == null) return;
    for (PortForwarder pf : pfs) {
      pf.stop();
    }
    pfs.clear();
  }
}
