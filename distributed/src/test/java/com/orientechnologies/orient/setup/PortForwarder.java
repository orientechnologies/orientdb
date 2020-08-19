package com.orientechnologies.orient.setup;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.Config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;

import static com.orientechnologies.orient.setup.TestSetupUtil.log;

public class PortForwarder {
  private KubectlPortForwarder pf;
  private String pfId;

  public PortForwarder(String namespace, String podName, int targetPort)
      throws IOException, ApiException {
    pfId = String.format("%s/%s:%d", namespace, podName, targetPort);
    ApiClient client = Config.defaultClient();
    Configuration.setDefaultApiClient(client);
    pf = new KubectlPortForwarder(namespace, podName, targetPort);
  }

  public int start() throws IOException {
    int localPort = pf.start();
    log("  Forwarding port %s -> localhost:%d...", pfId, localPort);
    return localPort;
  }

  public void stop() {
    pf.stop();
  }

  private class KubectlPortForwarder {
    private String namespace, podName;
    private int targetPort;
    private Process process;

    public KubectlPortForwarder(String namespace, String podName, int targetPort) {
      this.namespace = namespace;
      this.podName = podName;
      this.targetPort = targetPort;
    }

    public int start() throws IOException {
      for (int i = 0; i < 5; i++) {
        int port = getFreePort();
        String cmd =
            String.format(
                "kubectl -n %s port-forward %s %d:%d", namespace, podName, port, targetPort);
        log("  Running command: " + cmd);
        process = Runtime.getRuntime().exec(cmd);
        boolean stdout = false, stderr = false;
        // todo: have a timeout
        while (!stdout && !stderr) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
          stdout = process.getInputStream().available() > 0;
          stderr = process.getErrorStream().available() > 0;
        }
        if (stderr) {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getErrorStream()));
          log("  Error setting up kubectl port-forward: " + reader.readLine());
        } else if (stdout) {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
          log("  Output of kubectl port-forward: " + reader.readLine());
          if (targetPort != 2424) {
            return port;
          }
          // check if working
          OrientDB remote =
              new OrientDB(
                  String.format("remote:localhost:%d", port),
                  "root",
                  "test",
                  OrientDBConfig.defaultConfig());
          try {
            remote.list();
            return port;
          } catch (Exception e) {
          } finally {
            remote.close();
          }
        }
        process.destroy();
        log("  Trying kubectl again!");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
        }
      }
      throw new TestSetupException("Cannot setup kubectl port-forward.");
    }

    public void stop() {
      process.destroy();
    }

    private int getFreePort() {
      try (ServerSocket ss = new ServerSocket(0)) {
        ss.setReuseAddress(true);
        return ss.getLocalPort();
      } catch (IOException e) {
        throw new TestSetupException("Error getting a free port.", e);
      }
    }
  }
}
