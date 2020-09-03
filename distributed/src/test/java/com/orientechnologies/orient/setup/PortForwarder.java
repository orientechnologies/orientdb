package com.orientechnologies.orient.setup;

import static com.orientechnologies.orient.setup.TestSetupUtil.log;
import static com.orientechnologies.orient.setup.TestSetupUtil.sleep;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.Config;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;

/**
 * Uses 'kubectl port-forward' to expose the port and therefore needs 'kubectl' binary. While not a
 * reliable way of exposing services in general, for testing this should be good enough, considering
 * it will work everywhere and the alternative involves setting up NodePort service, or a
 * combination of LoadBalancer service and ingress (for HTTP) depending on where/how the Kubernetes
 * cluster is setup and runs.
 */
public class PortForwarder {
  public static int MAX_PORT_FORWARD_RETRY = 5;
  public static int PORT_FORWARD_RETRY_INTERVAL_SECONDS = 5;
  public static int PORT_FORWARD_TIMEOUT_SECONDS = 10;

  private KubectlPortForwarder pf;
  private String namespace;
  private String podName;
  private int targetPort;
  private boolean isBinary;
  private String serverUser;
  private String serverPass;

  public PortForwarder(
      String namespace, String podName, int targetPort, boolean isBinary, String user, String pass)
      throws IOException {
    this.namespace = namespace;
    this.podName = podName;
    this.targetPort = targetPort;
    this.serverUser = user;
    this.serverPass = pass;
    this.isBinary = isBinary;
    ApiClient client = Config.defaultClient();
    Configuration.setDefaultApiClient(client);
    pf = new KubectlPortForwarder();
  }

  public int start() throws IOException {
    int localPort = pf.start();
    log("  Forwarding port %s/%s:%d -> localhost:%d...", namespace, podName, targetPort, localPort);
    return localPort;
  }

  public void stop() {
    pf.stop();
  }

  private class KubectlPortForwarder {
    private Process process;

    public int start() throws IOException {
      for (int i = 0; i < MAX_PORT_FORWARD_RETRY; i++) {
        int port = getFreePort();
        process = runKubectlPortForward(port);
        boolean stdoutAvailable = false, stderrAvailable = false;
        long timeout = System.currentTimeMillis() + PORT_FORWARD_TIMEOUT_SECONDS * 1000;
        while (System.currentTimeMillis() < timeout && !stdoutAvailable && !stderrAvailable) {
          sleep(1);
          stdoutAvailable = process.getInputStream().available() > 0;
          stderrAvailable = process.getErrorStream().available() > 0;
        }
        if (stderrAvailable) {
          log("  Error setting up kubectl port-forward: " + readProcessErrorOutput(process));
        } else if (stdoutAvailable) {
          log("  Output of kubectl port-forward: " + readProcessOutput(process));
          // do a check only for binary port
          if (!isBinary) {
            return port;
          } else if (binaryPortForwardCheck(port)) {
            return port;
          }
        }
        process.destroy();
        log("  Trying kubectl again!");
        sleep(PORT_FORWARD_RETRY_INTERVAL_SECONDS);
      }
      throw new TestSetupException("Cannot setup kubectl port-forward.");
    }

    public void stop() {
      process.destroy();
    }

    private boolean binaryPortForwardCheck(int port) {
      // check if working
      OrientDB remote =
          new OrientDB(
              String.format("remote:localhost:%d", port),
              serverUser,
              serverPass,
              OrientDBConfig.defaultConfig());
      try {
        remote.list();
        return true;
      } catch (Exception e) {
      } finally {
        remote.close();
      }
      return false;
    }

    private int getFreePort() {
      try (ServerSocket ss = new ServerSocket(0)) {
        ss.setReuseAddress(true);
        return ss.getLocalPort();
      } catch (IOException e) {
        throw new TestSetupException("Error getting a free port.", e);
      }
    }

    private Process runKubectlPortForward(int localPort) throws IOException {
      String cmd =
          String.format(
              "kubectl -n %s port-forward %s %d:%d", namespace, podName, localPort, targetPort);
      log("  Running command: " + cmd);
      return Runtime.getRuntime().exec(cmd);
    }

    private String readProcessOutput(Process p) throws IOException {
      return readLineFromInputStream(p.getInputStream());
    }

    private String readProcessErrorOutput(Process p) throws IOException {
      return readLineFromInputStream(p.getErrorStream());
    }

    private String readLineFromInputStream(InputStream is) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      return reader.readLine();
    }
  }
}
