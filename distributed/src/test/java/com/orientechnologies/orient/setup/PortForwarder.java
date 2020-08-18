package com.orientechnologies.orient.setup;

import com.google.common.io.ByteStreams;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.Config;
import org.joda.time.DateTime;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class PortForwarder {
//  private Server pf;
  private KubectlPortForwarder pf;
  private String namespace, podName;
  private int targetPort;
  private String pfId;

  public PortForwarder(String namespace, String podName, int targetPort)
      throws IOException, ApiException {
    this.namespace = namespace;
    this.podName = podName;
    this.targetPort = targetPort;
    pfId = String.format("%s/%s:%d", namespace, podName, targetPort);
    ApiClient client = Config.defaultClient();
    Configuration.setDefaultApiClient(client);
//    PortForward forward = new PortForward();
//    List<Integer> ports = new ArrayList<>();
//    ports.add(targetPort);
//    final PortForward.PortForwardResult result = forward.forward(namespace, podName, ports);
//    pf = new Server(result.getInputStream(targetPort), result.getOutboundStream(targetPort));
    pf = new KubectlPortForwarder(namespace, podName, targetPort);
  }

  public int start() throws IOException {
    int localPort = pf.start();
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.printf("Forwarding port %s -> localhost:%d...\n", pfId, localPort);
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
        System.out.println("Running command: " + cmd);
        process = Runtime.getRuntime().exec(cmd);
        boolean stdout = false, stderr = false;
        System.out.printf("Waiting for command to run '%s' ...\n", cmd);
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
          System.out.println("error setting up kubectl: " + reader.readLine());
        } else if (stdout) {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
          System.out.println("Output of kubectl: " + reader.readLine());
          if (targetPort != 2424) {
            return port;
          }
          // check if working
          OrientDB remote = new OrientDB(String.format("remote:localhost:%d", port), "root", "test", OrientDBConfig.defaultConfig());
          try {
            remote.list();
            return port;
          } catch (Exception e) {
          } finally {
            remote.close();
          }
        }
        process.destroy();
        System.out.println("Trying kubectl again!");
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

  private class Server {
    private Thread inputCopier;
    private Thread outputCopier;
    private Thread server;
    private ServerSocket ss;
    private InputStream targetInputStream;
    private OutputStream targetOutputStream;
    private volatile boolean active;

    public Server(InputStream targetInputStream, OutputStream targetOutputStream)
        throws IOException {
      ss = new ServerSocket(0);
      this.targetInputStream = targetInputStream;
      this.targetOutputStream = targetOutputStream;
    }

    public int start() {
      active = true;
      server =
          new Thread(
              () -> {
                try {
                  runCopiers();
                } catch (IOException e) {
                  throw new TestSetupException(e);
                }
              });
      server.start();
      return ss.getLocalPort();
    }

    private void runCopiers() throws IOException {
      System.out.printf("Waiting for requests on port %d.\n", ss.getLocalPort());
      while (active) {
        final Socket s = ss.accept();

        inputCopier =
            new Thread(
                () -> {
                  try {
                    ByteStreams.copy(targetInputStream, s.getOutputStream());
                  } catch (Exception e) {
                    System.err.println("Error while copying target input stream.");
                    e.printStackTrace();
                    throw new TestSetupException(e);
                  }
                  System.out.printf("Exiting target input copier %s->%s.\n", pfId, s.getPort());
                });
        outputCopier =
            new Thread(
                () -> {
                  try {
                    ByteStreams.copy(s.getInputStream(), targetOutputStream);
                  } catch (Exception e) {
                    System.err.println("Error while copying local input stream.");
                    e.printStackTrace();
                    throw new TestSetupException(e);
                  }
                  System.out.printf("Exiting local input copier %s->%s.\n", pfId, s.getPort());
                });

        inputCopier.start();
        outputCopier.start();
        System.out.printf("Started copying threads for %s->%d.\n", pfId, s.getPort());
      }
      System.out.printf("Stopping port forwarder %s...\n", pfId);
    }

    public void stop() throws IOException {
      active = false;
      ss.close();
    }
  }
}
