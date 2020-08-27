## Running distributed integration tests on Kubernetes

To run ITs on Kubernetes you need to have kubectl installed, and you must provide a kubeconfig file. Make sure the installed kubectl is [compatible](https://kubernetes.io/docs/setup/release/version-skew-policy/#kubectl) with the Kubernetes cluster.

To run the integration tests on Kubernetes you must use the Maven `it-k8s` build profile.

Run a single test:
```
mvn failsafe:integration-test -Dit.test=BasicSyncIT -P it-k8s -DkubeConfig=/path/to/kubeconfig
```

To run all integration tests that can be run on Kubernetes (defined in the build profile):
```
mvn failsafe:integration-test -P it-k8s -DkubeConfig=/path/to/kubeconfig
```

The Kubernetes namespace used for the tests, is defined in the build profile and must exist before running the tests. You can use a different namespace by adding `-DkubernetesNamespace=name` to the above commands. The namespace and RBACs created by the tests, are not removed after the tests finish executing.

The current OrientDB Docker image (3.1.1) does not support node discovery on Kubernetes. To run the tests you need to build the Docker image (under `distribution/docker/Dockerfile`) from the [3.2.0-Snapshot](https://oss.sonatype.org/content/repositories/snapshots/com/orientechnologies/orientdb-community/3.2.0-SNAPSHOT/orientdb-community-3.2.0-20200814.124313-31.tar.gz) and provide the image name with `-DorientdbDockerImage=repository/image:tag`. Alternatively, you can use `-DorientdbDockerImage=pxsalehi/orientdb:3.1.2`.

Volume size and storage classes used for the PVs, can also be configured via the `it-k8s` build profile properties.