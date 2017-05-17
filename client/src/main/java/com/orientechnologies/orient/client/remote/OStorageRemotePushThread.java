package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by tglman on 11/01/17.
 */
public class OStorageRemotePushThread extends Thread {

  private ORemotePushHandler storage;
  private OChannelBinary     network;
  private BlockingQueue<OSubscribeResponse> blockingQueue = new SynchronousQueue<>();
  private volatile OBinaryRequest currentRequest;

  public OStorageRemotePushThread(ORemotePushHandler storage, String host) {
    this.storage = storage;
    network = storage.getNetwork(host);
  }

  @Override
  public void run() {

    try {
      while (true) {
        network.setWaitResponseTimeout();
        byte res = network.readByte();
        if (res == OChannelBinaryProtocol.RESPONSE_STATUS_OK) {
          int currentSessionId = network.readInt();
          byte[] token = network.readBytes();
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
          OBinaryPushRequest request = storage.createPush(push);
          request.read(network);
          OBinaryPushResponse response = request.execute(storage);
          synchronized (this) {
            //TODO DEFINE A NUMBER
            network.writeByte(OChannelBinaryProtocol.REQUEST_OK_PUSH);
            //session
            network.writeInt(-1);
            response.write(network);
          }
        }
      }
    } catch (SocketException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public synchronized <T extends OBinaryResponse> T subscribe(OBinaryRequest<T> request, OStorageRemoteSession session) {
    try {
      this.currentRequest = new OSubscribeRequest(request.getCommand(), request);
      ((OChannelBinaryAsynchClient) network).beginRequest(OChannelBinaryProtocol.SUBSCRIBE_PUSH, session);
      this.currentRequest.write(network, null);
      ((OChannelBinaryAsynchClient) network).endRequest();
      return (T) blockingQueue.take().getResponse();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void shutdown() {
    this.network.close();
    interrupt();
  }
}
