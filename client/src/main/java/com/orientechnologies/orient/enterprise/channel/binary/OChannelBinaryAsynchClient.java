/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemoteThreadLocal;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class OChannelBinaryAsynchClient extends OChannelBinaryClientAbstract {
  private final Condition                      readCondition = getLockRead().getUnderlying().newCondition();
  private final int                            maxUnreadResponses;
  private volatile boolean                     channelRead   = false;
  private volatile OAsynchChannelServiceThread serviceThread;

  public OChannelBinaryAsynchClient(final String remoteHost, final int remotePort, final String iDatabaseName,
      final OContextConfiguration iConfig, final int iProtocolVersion) throws IOException {
    this(remoteHost, remotePort, iDatabaseName, iConfig, iProtocolVersion, null);
  }

  public OChannelBinaryAsynchClient(final String remoteHost, final int remotePort, final String iDatabaseName,
      final OContextConfiguration iConfig, final int protocolVersion, final ORemoteServerEventListener asynchEventListener)
          throws IOException {
    super(remoteHost, remotePort, iDatabaseName, iConfig, protocolVersion);
    maxUnreadResponses = OGlobalConfiguration.NETWORK_BINARY_READ_RESPONSE_MAX_TIMES.getValueAsInteger();

    if (asynchEventListener != null)
      serviceThread = new OAsynchChannelServiceThread(asynchEventListener, this);
  }

  public void beginRequest(byte iCommand, OStorageRemoteThreadLocal.OStorageRemoteSession session, byte[] token)
      throws IOException {
    writeByte(iCommand);
    writeInt(session.sessionId);
    if (token != null) {
      if (!session.has(this)) {
        writeBytes(token);
        session.add(this);
      } else
        writeBytes(new byte[] {});
    }
  }

  public byte[] beginResponse(final int iRequesterId, final boolean token) throws IOException {
    return beginResponse(iRequesterId, timeout, token);
  }

  public byte[] beginResponse(final int iRequesterId, final long iTimeout, final boolean token) throws IOException {
    try {
      int unreadResponse = 0;
      final long startClock = iTimeout > 0 ? System.currentTimeMillis() : 0;

      // WAIT FOR THE RESPONSE
      do {
        if (iTimeout <= 0)
          acquireReadLock();
        else if (!getLockRead().tryAcquireLock(iTimeout, TimeUnit.MILLISECONDS))
          throw new OTimeoutException("Cannot acquire read lock against channel: " + this);

        boolean readLock = true;

        if (!isConnected()) {
          releaseReadLock();
          throw new IOException("Channel is closed");
        }

        if (!channelRead) {
          channelRead = true;

          try {
            setWaitResponseTimeout();
            currentStatus = readByte();
            currentSessionId = readInt();

            if (debug)
              OLogManager.instance().debug(this, "%s - Read response: %d-%d", socket.getLocalAddress(), (int) currentStatus,
                  currentSessionId);

          } catch (IOException e) {
            // UNLOCK THE RESOURCE AND PROPAGATES THE EXCEPTION
            channelRead = false;
            readCondition.signalAll();
            releaseReadLock();
            readLock = false;

            throw e;
          } finally {
            setReadResponseTimeout();
          }
        }

        if (currentSessionId == iRequesterId)
          // IT'S FOR ME
          break;

        try {
          if (debug)
            OLogManager.instance().debug(this, "%s - Session %d skip response, it is for %d", socket.getLocalAddress(),
                iRequesterId, currentSessionId);

          if (iTimeout > 0 && (System.currentTimeMillis() - startClock) > iTimeout) {
            // CLOSE THE SOCKET TO CHANNEL TO AVOID FURTHER DIRTY DATA
            close();
            readLock = false;

            throw new OTimeoutException("Timeout on reading response from the server "
                + (socket != null ? socket.getRemoteSocketAddress() : "") + " for the request " + iRequesterId);
          }

          // IN CASE OF TOO MUCH TIME FOR READ A MESSAGE, ASYNC THREAD SHOULD NOT BE INCLUDE IN THIS CHECK
          if (unreadResponse > maxUnreadResponses && iRequesterId != Integer.MIN_VALUE) {
            if (debug)
              OLogManager.instance().info(this, "Unread responses %d > %d, consider the buffer as dirty: clean it", unreadResponse,
                  maxUnreadResponses);

            close();
            readLock = false;

            throw new IOException("Timeout on reading response");
          }

          readCondition.signalAll();

          if (debug)
            OLogManager.instance().debug(this, "Session %d is going to sleep...", iRequesterId);

          final long start = System.currentTimeMillis();

          if (this.serviceThread == null || iRequesterId != Integer.MIN_VALUE && this.currentSessionId != Integer.MIN_VALUE) {
            throw new OIOException("Found a session id " + this.currentSessionId + " not expected, possible wrong data on socket");
          } else
            // WAIT MAX 30  SEC FOR THE ASYNC THREAD TO READ THE RESPONSE
          if (!readCondition.await(30000, TimeUnit.MILLISECONDS)) {
            //SOMETHING WENT WRONG IN THE ASYNC THREAD
            throw new OIOException("Timeout on push messaged reading by async thread");
          }

          if (debug) {
            final long now = System.currentTimeMillis();
            OLogManager.instance().debug(this, "Waked up: slept %dms, checking again from %s for session %d", (now - start),
                socket.getLocalAddress(), iRequesterId);
          }

          unreadResponse++;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();

        } finally {
          if (readLock)
            releaseReadLock();
        }
      } while (true);

      if (debug)
        OLogManager.instance().debug(this, "%s - Session %d handle response", socket.getLocalAddress(), iRequesterId);
      byte[] tokenBytes;
      if (token)
        tokenBytes = this.readBytes();
      else
        tokenBytes = null;
      handleStatus(currentStatus, currentSessionId);
      return tokenBytes;
    } catch (OLockException e) {
      Thread.currentThread().interrupt();
      // NEVER HAPPENS?
      OLogManager.instance().error(this, "Unexpected error on reading response from channel", e);
    }
    return null;
  }

  public void endResponse() throws IOException {
    channelRead = false;

    // WAKE UP ALL THE WAITING THREADS
    try {
      readCondition.signalAll();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      OLogManager.instance().debug(this, "Error on signaling waiting clients after reading response");
    }
    try {
      releaseReadLock();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      OLogManager.instance().debug(this, "Error on unlocking network channel after reading response");
    }

  }

  @Override
  public void close() {
    if (getLockRead().tryAcquireLock())
      try {
        readCondition.signalAll();
      } finally {
        releaseReadLock();
      }

    try {
      super.close();
    } catch (Exception e) {
      // IGNORE IT
    }

    if (serviceThread != null) {
      final OAsynchChannelServiceThread s = serviceThread;
      serviceThread = null;
      if (s != null)
        // CHECK S BECAUSE IT COULD BE CONCURRENTLY RESET
        s.sendShutdown();
    }
  }
}
