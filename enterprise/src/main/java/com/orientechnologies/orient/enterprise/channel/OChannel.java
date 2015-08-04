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
package com.orientechnologies.orient.enterprise.channel;

import com.orientechnologies.common.concur.lock.OAdaptiveLock;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicLong;

public abstract class OChannel extends OListenerManger<OChannelListener> {
  private static final OProfiler  PROFILER                     = Orient.instance().getProfiler();
  private static final AtomicLong metricGlobalTransmittedBytes = new AtomicLong();
  private static final AtomicLong metricGlobalReceivedBytes    = new AtomicLong();
  private static final AtomicLong metricGlobalFlushes          = new AtomicLong();
  private final OAdaptiveLock     lockRead                     = new OAdaptiveLock();
  private final OAdaptiveLock     lockWrite                    = new OAdaptiveLock();
  public volatile Socket          socket;
  public InputStream              inStream;
  public OutputStream             outStream;
  public int                      socketBufferSize;
  protected long                  timeout;
  private long                    metricTransmittedBytes       = 0;
  private long                    metricReceivedBytes          = 0;
  private long                    metricFlushes                = 0;
  private String                  profilerMetric;

  static {
    final String profilerMetric = PROFILER.getProcessMetric("network.channel.binary");

    PROFILER.registerHookValue(profilerMetric + ".transmittedBytes", "Bytes transmitted to all the network channels",
        METRIC_TYPE.SIZE, new OProfilerHookValue() {
          public Object getValue() {
            return metricGlobalTransmittedBytes.get();
          }
        });
    PROFILER.registerHookValue(profilerMetric + ".receivedBytes", "Bytes received from all the network channels", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          public Object getValue() {
            return metricGlobalReceivedBytes.get();
          }
        });
    PROFILER.registerHookValue(profilerMetric + ".flushes", "Number of times the network channels have been flushed",
        METRIC_TYPE.COUNTER, new OProfilerHookValue() {
          public Object getValue() {
            return metricGlobalFlushes.get();
          }
        });
  }

  public OChannel(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
    super(true);
    socket = iSocket;
    socketBufferSize = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_BUFFER_SIZE);
    socket.setTcpNoDelay(true);
  }

  public static String getLocalIpAddress(final boolean iFavoriteIp4) throws SocketException {
    String bestAddress = null;
    final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    while (interfaces.hasMoreElements()) {
      final NetworkInterface current = interfaces.nextElement();
      if (!current.isUp() || current.isLoopback() || current.isVirtual())
        continue;
      Enumeration<InetAddress> addresses = current.getInetAddresses();
      while (addresses.hasMoreElements()) {
        final InetAddress current_addr = addresses.nextElement();
        if (current_addr.isLoopbackAddress())
          continue;

        if (bestAddress == null || (iFavoriteIp4 && current_addr instanceof Inet4Address))
          // FAVORITE IP4 ADDRESS
          bestAddress = current_addr.getHostAddress();
      }
    }
    return bestAddress;
  }

  public void acquireWriteLock() {
    lockWrite.lock();
  }

  public void releaseWriteLock() {
    lockWrite.unlock();
  }

  public void acquireReadLock() {
    lockRead.lock();
  }

  public void releaseReadLock() {
    lockRead.unlock();
  }

  public void flush() throws IOException {
    if (outStream != null)
      outStream.flush();
  }

  public OAdaptiveLock getLockRead() {
    return lockRead;
  }

  public OAdaptiveLock getLockWrite() {
    return lockWrite;
  }

  public synchronized void close() {
    PROFILER.unregisterHookValue(profilerMetric + ".transmittedBytes");
    PROFILER.unregisterHookValue(profilerMetric + ".receivedBytes");
    PROFILER.unregisterHookValue(profilerMetric + ".flushes");

    try {
      if (socket != null) {
        socket.close();
        socket = null;
      }
    } catch (Exception e) {
      OLogManager.instance().debug(this, "Error during socket close", e);
    }

    try {
      if (inStream != null) {
        inStream.close();
        inStream = null;
      }
    } catch (Exception e) {
      OLogManager.instance().debug(this, "Error during closing of input stream", e);
    }

    try {
      if (outStream != null) {
        outStream.close();
        outStream = null;
      }
    } catch (Exception e) {
      OLogManager.instance().debug(this, "Error during closing of output stream", e);
    }

    for (OChannelListener l : getListenersCopy())
      try {
        l.onChannelClose(this);
      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error during closing of channel close listener", e);
      }

    lockRead.close();
    lockWrite.close();

    resetListeners();
  }

  public void connected() {
    final String dictProfilerMetric = PROFILER.getProcessMetric("network.channel.binary.*");

    profilerMetric = PROFILER.getProcessMetric("network.channel.binary." + socket.getRemoteSocketAddress().toString()
        + socket.getLocalPort() + "".replace('.', '_'));

    PROFILER.registerHookValue(profilerMetric + ".transmittedBytes", "Bytes transmitted to a network channel", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          public Object getValue() {
            return metricTransmittedBytes;
          }
        }, dictProfilerMetric + ".transmittedBytes");
    PROFILER.registerHookValue(profilerMetric + ".receivedBytes", "Bytes received from a network channel", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          public Object getValue() {
            return metricReceivedBytes;
          }
        }, dictProfilerMetric + ".receivedBytes");
    PROFILER.registerHookValue(profilerMetric + ".flushes", "Number of times the network channel has been flushed",
        METRIC_TYPE.COUNTER, new OProfilerHookValue() {
          public Object getValue() {
            return metricFlushes;
          }
        }, dictProfilerMetric + ".flushes");
  }

  @Override
  public String toString() {
    return socket != null ? socket.getRemoteSocketAddress().toString() : "Not connected";
  }

  public String getLocalSocketAddress() {
    return socket != null ? socket.getLocalSocketAddress().toString() : "?";
  }

  protected void updateMetricTransmittedBytes(final int iDelta) {
    metricGlobalTransmittedBytes.addAndGet(iDelta);
    metricTransmittedBytes += iDelta;
  }

  protected void updateMetricReceivedBytes(final int iDelta) {
    metricGlobalReceivedBytes.addAndGet(iDelta);
    metricReceivedBytes += iDelta;
  }

  protected void updateMetricFlushes() {
    metricGlobalFlushes.incrementAndGet();
    metricFlushes++;
  }

}
