package com.orientechnologies.orient.testutil;

import com.google.common.io.ByteStreams;
import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiException;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class NodePortForward {
  public static void create(String namespace, String podName, int localPort, int remotePort)
      throws IOException, ApiException {
    PortForward forward = new PortForward();
    List<Integer> ports = new ArrayList<>();
    ports.add(localPort);
    ports.add(remotePort);
    final PortForward.PortForwardResult result = forward.forward(namespace, podName, ports);

    ServerSocket ss = new ServerSocket(localPort);

    final Socket s = ss.accept();
    System.out.println("Connected!");

    new Thread(
            () -> {
              try {
                ByteStreams.copy(result.getInputStream(localPort), s.getOutputStream());
              } catch (IOException ex) {
                ex.printStackTrace();
              } catch (Exception ex) {
                ex.printStackTrace();
              }
            })
        .start();

    new Thread(
            () -> {
              try {
                ByteStreams.copy(s.getInputStream(), result.getOutboundStream(localPort));
              } catch (IOException ex) {
                ex.printStackTrace();
              } catch (Exception ex) {
                ex.printStackTrace();
              }
            })
        .start();
  }
}
