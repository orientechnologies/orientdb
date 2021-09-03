## Running distributed integration tests on Kubernetes

To run ITs on Kubernetes you need to have kubectl installed, and you must provide a kubeconfig file. Make sure the installed kubectl is [compatible](https://kubernetes.io/docs/setup/release/version-skew-policy/#kubectl) with the Kubernetes cluster.

> Only few ITs can be run on Kubernetes due to development effort, but mostly due to a requied running `OServer` instances.

To run the integration tests on Kubernetes you must use the Maven `it-k8s` build profile.

Run a single test (e.g. on OrientDB 3.1.3):
```
mvn failsafe:integration-test -Dit.test=BasicSyncIT -P it-k8s -DkubeConfig=/path/to/kubeconfig -DorientdbDockerImage=orientdb:3.1.3
```

To run all integration tests that can be run on Kubernetes (defined in the build profile):
```
mvn failsafe:integration-test -P it-k8s -DkubeConfig=/path/to/kubeconfig -DorientdbDockerImage=orientdb:3.1.3
```

To run tests on a specific build, you must build a Docker image and pass it via the `-DorientdbDockerImage=repository/image:tag` flag.

The Kubernetes namespace used for the tests, is defined in the build profile and must exist before running the tests. You can use a different namespace by adding `-DkubernetesNamespace=name` to the above commands. The namespace and RBACs created by the tests, are not removed after the tests finish executing.

OrientDB Docker images before 3.1.3 do not support node discovery on Kubernetes and therefore do not support this setup.

Volume size and storage classes used for the PVs, can also be configured via the `it-k8s` build profile properties.

Please note that due to issues that the official Kuberbenetes Java client has with some versions of JDK8 (broken pipe error), you should use JDK9 or later to run the tests. 

**Next steps**

- Use the Kubernetes setup for more sophisticated tests of `distributed`, e.g., add delays.
