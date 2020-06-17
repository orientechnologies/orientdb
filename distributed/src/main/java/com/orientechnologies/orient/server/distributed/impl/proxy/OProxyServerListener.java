package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.common.log.OLogManager;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class OProxyServerListener extends Thread {
  private final OProxyServer server;
  private final int localPort;
  private final int remotePort;

  private List<OProxyChannel> channels = new ArrayList<OProxyChannel>();
  private ServerSocket localSocket;
  private boolean running = true;

  public OProxyServerListener(
      final OProxyServer server, final int localPort, final int remotePort) {
    this.server = server;
    this.localPort = localPort;
    this.remotePort = remotePort;
  }

  public void run() {
    // WAIT THE REMOTE SOCKET IS AVAILABLE FIRST. IN THIS WAY THE LOCAL SOCKET IS CREATED ONLY AFTER
    // THE REMOTE SOCKET IS
    // AVAILABLE. THIS ALLOWS TO RETURN A CONNECTION REFUSED TO THE CLIENT
    OLogManager.instance()
        .info(
            this,
            "Proxy server: local port %d is waiting for the remote port %s:%d to be available...",
            localPort,
            server.getRemoteHost(),
            remotePort);

    if (server.isWaitUntilRemotePortsAreOpen()) {
      while (running) {
        try {
          final Socket remoteSocket = connectTargetServer();
          remoteSocket.close();
          break;

        } catch (Exception e) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            return;
          }
        }
      }
    }

    OLogManager.instance()
        .info(
            this,
            "Proxy server: remote port %s:%d is available, creating the channel...",
            server.getRemoteHost(),
            remotePort);

    try {
      localSocket = new ServerSocket(localPort);
    } catch (Exception e) {
      OLogManager.instance()
          .error(
              this,
              "Proxy server: error on creating local socket for proxy channel %d->%s:%d",
              e,
              localPort,
              server.getRemoteHost(),
              remotePort);
    }

    while (running && !localSocket.isClosed())
      try {
        final Socket sourceSocket = localSocket.accept();

        final OProxyChannel channel = new OProxyChannel(this, sourceSocket, localPort, remotePort);
        channels.add(channel);
        channel.start();

      } catch (BindException e) {
        OLogManager.instance()
            .error(
                this,
                "Proxy server: error on listening port %d->%s:%d",
                e,
                localPort,
                server.getRemoteHost(),
                remotePort);
        break;
      } catch (SocketException e) {
        OLogManager.instance()
            .debug(this, "Proxy server: listening port %d is closed", e, localPort);
        break;
      } catch (Exception e) {
        OLogManager.instance()
            .error(
                this,
                "Proxy server: closing proxy server %d->%s:%d",
                e,
                localPort,
                server.getRemoteHost(),
                remotePort);
        break;
      }

    shutdown();
  }

  public Socket connectTargetServer() throws IOException {
    final Socket targetSocket = new Socket(server.getRemoteHost(), remotePort);
    return targetSocket;
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

  public void shutdown() {
    running = false;

    if (localSocket != null)
      try {
        localSocket.close();
      } catch (IOException e) {
        // IGNORE IT
      }

    for (OProxyChannel t : channels) {
      t.sendShutdown();
    }
  }

  public OProxyServer getServer() {
    return server;
  }
}
