/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Implementation that supports multiple client requests.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OChannelBinaryAsynch extends OChannelBinary {
  private final Condition  readCondition = lockRead.getUnderlying().newCondition();
  private volatile boolean channelRead   = false;
  private byte             currentStatus;
  private int              currentSessionId;
  private final int        maxUnreadResponses;

  public OChannelBinaryAsynch(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
    super(iSocket, iConfig);
    maxUnreadResponses = OGlobalConfiguration.NETWORK_BINARY_READ_RESPONSE_MAX_TIMES.getValueAsInteger();
  }

  public void beginRequest() {
    acquireWriteLock();
  }

  public void endRequest() throws IOException {
    flush();
    releaseWriteLock();
  }

  public void beginResponse(final int iRequesterId) throws IOException {
    beginResponse(iRequesterId, timeout);
  }

  public void beginResponse(final int iRequesterId, final long iTimeout) throws IOException {
    try {
      int unreadResponse = 0;
      final long startClock = iTimeout > 0 ? System.currentTimeMillis() : 0;

      // WAIT FOR THE RESPONSE
      do {
        if (iTimeout <= 0)
          acquireReadLock();
        else if (!lockRead.getUnderlying().tryLock(iTimeout, TimeUnit.MILLISECONDS))
          throw new OTimeoutException("Cannot acquire read lock against channel: " + this);

        if (!channelRead) {
          channelRead = true;

          try {
            currentStatus = readByte();
            currentSessionId = readInt();

            if (debug)
              OLogManager.instance().debug(this, "%s - Read response: %d-%d", socket.getLocalAddress(), (int) currentStatus,
                  currentSessionId);

          } catch (IOException e) {
            // UNLOCK THE RESOURCE AND PROPAGATES THE EXCEPTION
            channelRead = false;
            readCondition.signalAll();
            lockRead.unlock();
            throw e;
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
            throw new OTimeoutException("Timeout on reading response from the server "
                + (socket != null ? socket.getRemoteSocketAddress() : "") + " for the request " + iRequesterId);
          }

          if (unreadResponse > maxUnreadResponses) {
            if (debug)
              OLogManager.instance().info(this, "Unread responses %d > %d, consider the buffer as dirty: clean it", unreadResponse,
                  maxUnreadResponses);

            close();
            throw new IOException("Timeout on reading response");
          }

          readCondition.signalAll();

          if (debug)
            OLogManager.instance().debug(this, "Session %d is going to sleep...", iRequesterId);

          final long start = System.currentTimeMillis();

          // WAIT 1 SECOND AND RETRY
          readCondition.await(1, TimeUnit.SECONDS);
          final long now = System.currentTimeMillis();

          if (debug)
            OLogManager.instance().debug(this, "Waked up: slept %dms, checking again from %s for session %d", (now - start),
                socket.getLocalAddress(), iRequesterId);

          if (now - start >= 1000)
            unreadResponse++;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();

        } finally {
          lockRead.unlock();
        }
      } while (true);

      if (debug)
        OLogManager.instance().debug(this, "%s - Session %d handle response", socket.getLocalAddress(), iRequesterId);

      handleStatus(currentStatus, currentSessionId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // NEVER HAPPENS?
      e.printStackTrace();
    }
  }

  public void endResponse() {
    channelRead = false;

    // WAKE UP ALL THE WAITING THREADS

    try {
      readCondition.signalAll();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      OLogManager.instance().debug(this, "Error on signaling waiting clients after reading response");
    }

    try {
      lockRead.unlock();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      OLogManager.instance().debug(this, "Error on unlocking network channel after reading response");
    }
  }

  @Override
  public void close() {
    if (lockRead.getUnderlying().tryLock())
      try {
        readCondition.signalAll();
      } finally {
        lockRead.unlock();
      }

    super.close();
  }

  @Override
  public void clearInput() throws IOException {
    lockRead.lock();
    try {
      super.clearInput();
    } finally {
      lockRead.unlock();
    }
  }
}
