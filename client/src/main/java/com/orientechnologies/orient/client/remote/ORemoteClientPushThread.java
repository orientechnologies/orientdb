package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OSubscribeRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeResponse;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/** Created by tglman on 11/01/17. */
public class ORemoteClientPushThread extends Thread {
  private static final OLogger logger =
      OLogManager.instance().logger(ORemoteClientPushThread.class);

  private final ORemotePushHandler pushHandler;
  private final String host;
  private final int retryDelay;
  private final long requestTimeout;
  private volatile OChannelBinary network;
  private final BlockingQueue<Object> blockingQueue = new SynchronousQueue<>();
  private volatile OBinaryRequest currentRequest;
  private volatile boolean shutDown;

  public ORemoteClientPushThread(
      ORemotePushHandler storage, String host, int retryDelay, long requestTimeout) {
    setDaemon(true);
    this.pushHandler = storage;
    this.host = host;
    network = storage.getNetwork(this.host);
    this.retryDelay = retryDelay;
    this.requestTimeout = requestTimeout;
  }

  public void handleException(Throwable throwable) {
    try {
      blockingQueue.put(throwable);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void run() {
    while (!Thread.interrupted() && !shutDown) {
      try {
        network.setWaitResponseTimeout();
        byte res = network.getChannelDataInput().readByte();
        if (res == OChannelBinaryProtocol.RESPONSE_STATUS_OK) {
          int currentSessionId = network.getChannelDataInput().readInt();
          byte[] token = network.getChannelDataInput().readBytes();
          byte messageId = network.getChannelDataInput().readByte();
          OBinaryResponse response = currentRequest.createResponse();
          response.read(network.getChannelDataInput());
          blockingQueue.put(response);
        } else if (res == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
          int currentSessionId = network.getChannelDataInput().readInt();
          byte[] token = network.getChannelDataInput().readBytes();
          byte messageId = network.getChannelDataInput().readByte();
          // TODO move handle status somewhere else
          ORemoteClient.handleStatus(res, network.getChannelDataInput(), this::handleException);
        } else {
          byte push = network.getChannelDataInput().readByte();
          OBinaryPushRequest request = pushHandler.createPush(push);
          request.read(network.getChannelDataInput());
          try {
            OBinaryPushResponse response = request.execute(pushHandler);
            if (response != null) {
              synchronized (this) {
                network.getChannelDataOutput().writeByte(OChannelBinaryProtocol.REQUEST_OK_PUSH);
                // session
                network.getChannelDataOutput().writeInt(-1);
                response.write(network.getChannelDataOutput());
              }
            }
          } catch (Exception e) {
            logger.error("Error executing push request", e);
          }
        }
      } catch (IOException | OException e) {
        pushHandler.onPushDisconnect(this.network, e);
        while (!isInterrupted()) {
          try {
            Thread.sleep(retryDelay);
          } catch (InterruptedException x) {
            interrupt();
          }
          if (!isInterrupted()) {
            try {
              synchronized (this) {
                this.network = null;
                this.network = pushHandler.getNetwork(this.host);
              }
              pushHandler.onPushReconnect(this.host);
              break;
            } catch (OIOException ex) {
              if (this.network != null) {
                pushHandler.onPushDisconnect(this.network, ex);
              }
              // Noting it just retry
            }
          }
        }
      } catch (InterruptedException e) {
        pushHandler.onPushDisconnect(this.network, e);
        interrupt();
      } catch (Throwable e) {
        logger.warn("Push thread error ", e);
        throw e;
      }
    }
  }

  public <T extends OBinaryResponse> T subscribe(
      OBinaryRequest<T> request, ORemoteClientSession session) {
    try {
      synchronized (this) {
        this.currentRequest = new OSubscribeRequest(request);
        String serverURL = ((OChannelBinaryAsynchClient) network).getServerURL();
        final ORemoteClientNodeSession nodeSession = session.getServerSession(serverURL);
        OChannelDataOutput output = network.getChannelDataOutput();
        if (nodeSession == null)
          throw new OIOException("Invalid session for URL '" + serverURL + "'");
        ORemoteClient.writeRequest(this.currentRequest, output, nodeSession);
        output.flush();
      }
      Object poll = blockingQueue.poll(requestTimeout, TimeUnit.MILLISECONDS);
      if (poll == null) return null;
      if (poll instanceof OSubscribeResponse) {
        return (T) ((OSubscribeResponse) poll).getResponse();
      } else if (poll instanceof RuntimeException) {
        throw (RuntimeException) poll;
      }
    } catch (IOException e) {
      logger.warn("Exception on subscribe", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  public void shutdown() {
    shutDown = true;
    interrupt();
    pushHandler.returnSocket(this.network);
  }
}
