package com.orientechnologies.orient.setup;

import com.google.common.io.ByteStreams;
import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class PortForwarder {
  private Server server;

  public PortForwarder(String namespace, String podName, int localPort, int targetPort)
      throws IOException, ApiException {
    ApiClient client = Config.defaultClient();
    Configuration.setDefaultApiClient(client);
    PortForward forward = new PortForward();
    List<Integer> ports = new ArrayList<>();
    ports.add(targetPort);
    final PortForward.PortForwardResult result = forward.forward(namespace, podName, ports);
    System.out.printf(
        "Forwarding port %s/%s:%d -> localhost:%d...\n", namespace, podName, targetPort, localPort);
    server = new Server(localPort, result.getInputStream(targetPort), result.getOutboundStream(targetPort));
  }

  public void start() {
    server.start();
  }

  public void stop() throws IOException {
    server.stop();
  }

  private class Server {
    private Thread inputCopier;
    private Thread outputCopier;
    private Thread server;
    private ServerSocket ss;
    private InputStream targetInputStream;
    private OutputStream targetOutputStream;

    public Server(int localPort, InputStream targetInputStream, OutputStream targetOutputStream)
        throws IOException {
      ss = new ServerSocket(localPort);
      this.targetInputStream = targetInputStream;
      this.targetOutputStream = targetOutputStream;
    }

    public void start() {
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
    }

    private void runCopiers() throws IOException {
      final Socket s = ss.accept();

      inputCopier =
          new Thread(
              () -> {
                try {
                  ByteStreams.copy(targetInputStream, s.getOutputStream());
                } catch (Exception e) {
                  throw new TestSetupException(e);
                }
              });
      outputCopier =
          new Thread(
              () -> {
                try {
                  ByteStreams.copy(s.getInputStream(), targetOutputStream);
                } catch (Exception e) {
                  throw new TestSetupException(e);
                }
              });

      inputCopier.start();
      outputCopier.start();
    }

    public void stop() throws IOException {
      if (inputCopier != null) inputCopier.interrupt();
      if (outputCopier != null) outputCopier.interrupt();
      if (server != null) server.interrupt();
      ss.close();
    }
  }
}
