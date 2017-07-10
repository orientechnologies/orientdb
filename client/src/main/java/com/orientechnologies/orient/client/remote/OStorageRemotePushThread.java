package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by tglman on 11/01/17.
 */
public class OStorageRemotePushThread extends Thread {

  private final ORemotePushHandler pushHandler;
  private final String             host;
  private final int                retryDelay;
  private       OChannelBinary     network;
  private BlockingQueue<OSubscribeResponse> blockingQueue = new SynchronousQueue<>();
  private volatile OBinaryRequest currentRequest;

  public OStorageRemotePushThread(ORemotePushHandler storage, String host, int retryDelay) {
    this.pushHandler = storage;
    this.host = host;
    network = storage.getNetwork(this.host);
    this.retryDelay = retryDelay;
  }

  @Override
  public void run() {
    while (!Thread.interrupted()) {
      try {
        network.setWaitResponseTimeout();
        byte res = network.readByte();
        if (res == OChannelBinaryProtocol.RESPONSE_STATUS_OK) {
          int currentSessionId = network.readInt();
          byte[] token = network.readBytes();
          byte messageId = network.readByte();
          OBinaryResponse response = currentRequest.createResponse();
          response.read(network, null);
          blockingQueue.put((OSubscribeResponse) response);
        } else if (res == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
          int currentSessionId = network.readInt();
          byte[] token = network.readBytes();
          //TODO move handle status somewhere else
          ((OChannelBinaryAsynchClient) network).handleStatus(res, currentSessionId);
        } else {
          byte push = network.readByte();
          OBinaryPushRequest request = pushHandler.createPush(push);
          request.read(network);
          OBinaryPushResponse response = request.execute(pushHandler);
          if (response != null) {
            synchronized (this) {
              network.writeByte(OChannelBinaryProtocol.REQUEST_OK_PUSH);
              //session
              network.writeInt(-1);
              response.write(network);
            }
          }
        }
      } catch (IOException e) {
        pushHandler.onPushDisconnect(e);
        while (!currentThread().isInterrupted()) {
          try {
            Thread.sleep(retryDelay);
          } catch (InterruptedException x) {
            currentThread().interrupt();
          }
          if (!currentThread().isInterrupted()) {
            try {
              synchronized (this) {
                try {
                  network.close();
                } catch (RuntimeException ignore) {
                  //IGNORE
                }
                network = pushHandler.getNetwork(this.host);
              }
              pushHandler.onPushReconnect(this.host);
              break;
            } catch (OIOException ex) {
              //Noting it just retry
            }
          }
        }
      } catch (InterruptedException e) {
        pushHandler.onPushDisconnect(e);
        currentThread().interrupt();
      }
    }
  }

  public synchronized <T extends OBinaryResponse> T subscribe(OBinaryRequest<T> request, OStorageRemoteSession session) {
    try {
      this.currentRequest = new OSubscribeRequest(request);
      ((OChannelBinaryAsynchClient) network).beginRequest(OChannelBinaryProtocol.SUBSCRIBE_PUSH, session);
      this.currentRequest.write(network, null);
      network.flush();
      return (T) blockingQueue.take().getResponse();
    } catch (IOException e) {
      OLogManager.instance().warn(this, "Exception on subscribe", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  public void shutdown() {
    interrupt();
    this.network.close();
  }
}
