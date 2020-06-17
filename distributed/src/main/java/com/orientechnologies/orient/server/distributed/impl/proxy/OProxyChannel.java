package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.common.log.OLogManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class OProxyChannel extends Thread {
  private final OProxyServerListener listener;
  private final Socket sourceSocket;
  private final int localPort;
  private final String remoteHost;
  private final int remotePort;
  private final InetSocketAddress sourceAddress;

  private Thread responseThread;
  private ServerSocket localSocket;
  private Socket targetSocket;
  private InputStream sourceInput;
  private OutputStream sourceOutput;
  private InputStream targetInput;
  private OutputStream targetOutput;
  private boolean running = true;
  protected long requestCount = 0;
  protected long responseCount = 0;

  public OProxyChannel(
      final OProxyServerListener listener,
      final Socket sourceSocket,
      int localPort,
      int remotePort) {
    this.listener = listener;
    this.sourceSocket = sourceSocket;
    this.localPort = localPort;
    this.remoteHost = listener.getServer().getRemoteHost();
    this.remotePort = remotePort;
    this.sourceAddress = ((InetSocketAddress) sourceSocket.getRemoteSocketAddress());

    OLogManager.instance()
        .info(
            this,
            "Proxy server: created channel %s:%d->[localhost:%d]->%s:%d",
            sourceAddress.getHostName(),
            sourceAddress.getPort(),
            localPort,
            listener.getServer().getRemoteHost(),
            remotePort);
  }

  @Override
  public void run() {
    try {
      try {
        if (listener.getServer().readTimeout > 0)
          sourceSocket.setSoTimeout(listener.getServer().readTimeout);

        sourceInput = sourceSocket.getInputStream();
        sourceOutput = sourceSocket.getOutputStream();

        targetSocket = listener.connectTargetServer();

        if (listener.getServer().readTimeout > 0)
          targetSocket.setSoTimeout(listener.getServer().readTimeout);

        targetInput = targetSocket.getInputStream();
        targetOutput = targetSocket.getOutputStream();

      } catch (IOException e) {
        OLogManager.instance()
            .error(
                this,
                "Proxy server: error on connecting to the remote server %s:%d",
                e,
                remoteHost,
                remotePort);
        return;
      }

      createResponseThread();

      try {
        final byte[] request = new byte[listener.getServer().bufferSize];
        while (running) {
          int bytesRead = 0;
          try {
            bytesRead = sourceInput.read(request);
          } catch (SocketTimeoutException e) {
          }

          if (bytesRead < 1) continue;

          requestCount++;

          listener.getServer().onMessage(true, localPort, remotePort, request, bytesRead);

          targetOutput.write(request, 0, bytesRead);
          targetOutput.flush();

          if (!listener.getServer().tracing.equalsIgnoreCase("none"))
            OLogManager.instance()
                .info(
                    this,
                    "Proxy channel: REQUEST(%d) %s:%d->[localhost:%d]->%s:%d = %d[%s]",
                    requestCount,
                    sourceAddress.getHostName(),
                    sourceAddress.getPort(),
                    localPort,
                    remoteHost,
                    remotePort,
                    bytesRead,
                    formatBytes(request, bytesRead));
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Proxy channel: error on reading request from port %d", e, localPort);
      }

    } finally {
      shutdown();
    }
  }

  public void shutdown() {
    running = false;

    if (localSocket != null)
      try {
        localSocket.close();
      } catch (IOException e) {
        // IGNORE IT
      }
    if (sourceInput != null)
      try {
        sourceInput.close();
      } catch (IOException e) {
        // IGNORE IT
      }
    if (sourceOutput != null)
      try {
        sourceOutput.close();
      } catch (IOException e) {
        // IGNORE IT
      }
    if (sourceSocket != null)
      try {
        sourceSocket.close();
      } catch (IOException e) {
        // IGNORE IT
      }
    if (targetSocket != null)
      try {
        targetSocket.close();
      } catch (IOException e) {
        // IGNORE IT
      }
    if (targetOutput != null)
      try {
        targetOutput.close();
      } catch (IOException e) {
      }
    if (responseThread != null)
      try {
        responseThread.join();
      } catch (InterruptedException e) {
        // IGNORE IT
      }
  }

  public void sendShutdown() {
    interrupt();

    shutdown();

    try {
      join();
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  protected void createResponseThread() {
    responseThread =
        new Thread() {
          public void run() {

            try {
              final byte[] response = new byte[listener.getServer().bufferSize];
              while (running) {
                int bytesRead = 0;
                try {
                  bytesRead = targetInput.read(response);
                } catch (SocketTimeoutException e) {
                }

                if (bytesRead < 1) continue;

                responseCount++;

                listener.getServer().onMessage(false, localPort, remotePort, response, bytesRead);

                sourceOutput.write(response, 0, bytesRead);
                sourceOutput.flush();

                if (!listener.getServer().tracing.equalsIgnoreCase("none"))
                  OLogManager.instance()
                      .info(
                          this,
                          "Proxy channel: RESPONSE(%d) %s:%d->[localhost:%d]->%s:%d = %d[%s]",
                          responseCount,
                          remoteHost,
                          remotePort,
                          localPort,
                          sourceAddress.getHostName(),
                          sourceAddress.getPort(),
                          bytesRead,
                          formatBytes(response, bytesRead));
              }
            } catch (IOException e) {
              OLogManager.instance()
                  .error(
                      this,
                      "Proxy channel: error on reading request from port %s:%d",
                      e,
                      remoteHost,
                      remotePort);
              running = false;
            }
          }
        };
    responseThread.start();
  }

  private String formatBytes(final byte[] request, final int total) {
    if ("none".equalsIgnoreCase(listener.getServer().tracing)) return "";

    final StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < total; ++i) {
      if (i > 0) buffer.append(',');

      if ("byte".equalsIgnoreCase(listener.getServer().tracing)) buffer.append(request[i]);
      else if ("hex".equalsIgnoreCase(listener.getServer().tracing))
        buffer.append(String.format("0x%x", request[i]));
    }
    return buffer.toString();
  }
}
