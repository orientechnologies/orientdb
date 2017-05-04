package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by tglman on 11/01/17.
 */
public class OStorageRemotePushThread extends Thread {

  private OStorageRemote             storage;
  private OChannelBinaryAsynchClient network;
  private BlockingQueue<OBinaryResponse> blockingQueue = new SynchronousQueue<>();
  private volatile OBinaryRequest currentRequest;

  public OStorageRemotePushThread(OStorageRemote storage) {
    this.storage = storage;
    network = storage.getNetwork(storage.getCurrentServerURL());
  }

  private OBinaryPushRequest createPush(byte type) {
    switch (type) {
    case OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG:
      return new OPushDistributedConfigurationRequest();
//    case OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY:
//    case OChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG:
//
//      return  new
    }
    return null;
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
          blockingQueue.put(response);
        } else if (res == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
          int currentSessionId = network.readInt();
          byte[] token = network.readBytes();
          network.handleStatus(res, currentSessionId);
        } else {
          byte push = network.readByte();
          OBinaryPushRequest request = createPush(push);
          request.read(network);
          OBinaryPushResponse response = request.execute(storage);
          synchronized (this) {
            //TODO DEFINE A NUMBER
            network.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_RESPONSE);
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

  public synchronized <T extends OBinaryResponse> T subscribe(OBinaryRequest<T> request, OStorageRemoteNodeSession nodeSession) {
    try {
      this.currentRequest = request;
      network.beginRequest(request.getCommand(), nodeSession);
      request.write(network, null);
      network.endRequest();
      return (T) blockingQueue.take();
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
