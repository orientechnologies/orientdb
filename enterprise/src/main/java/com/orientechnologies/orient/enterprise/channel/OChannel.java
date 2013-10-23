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
package com.orientechnologies.orient.enterprise.channel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.concur.lock.OAdaptiveLock;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelListener;

public abstract class OChannel extends OListenerManger<OChannelListener> {
  private static final OProfilerMBean PROFILER                     = Orient.instance().getProfiler();

  public Socket                       socket;

  public InputStream                  inStream;
  public OutputStream                 outStream;

  protected final OAdaptiveLock       lockRead                     = new OAdaptiveLock();
  protected final OAdaptiveLock       lockWrite                    = new OAdaptiveLock();
  protected long                      timeout;

  public int                          socketBufferSize;

  private long                        metricTransmittedBytes       = 0;
  private long                        metricReceivedBytes          = 0;
  private long                        metricFlushes                = 0;

  private static final AtomicLong     metricGlobalTransmittedBytes = new AtomicLong();
  private static final AtomicLong     metricGlobalReceivedBytes    = new AtomicLong();
  private static final AtomicLong     metricGlobalFlushes          = new AtomicLong();

  private String                      profilerMetric;

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
    socket = iSocket;
    socketBufferSize = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_BUFFER_SIZE);
    socket.setTcpNoDelay(true);
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
    outStream.flush();
  }

  public void close() {
    PROFILER.unregisterHookValue(profilerMetric + ".transmittedBytes");
    PROFILER.unregisterHookValue(profilerMetric + ".receivedBytes");
    PROFILER.unregisterHookValue(profilerMetric + ".flushes");

    try {
      if (socket != null)
        socket.close();
    } catch (IOException e) {
    }

    try {
      if (inStream != null)
        inStream.close();
    } catch (IOException e) {
    }

    try {
      if (outStream != null)
        outStream.close();
    } catch (IOException e) {
    }

    for (OChannelListener l : browseListeners())
      try {
        l.onChannelClose(this);
      } catch (Exception e) {
        // IGNORE ANY EXCEPTION
      }
  }

  public void connected() {
    profilerMetric = PROFILER.getProcessMetric("network.channel.binary." + socket.getRemoteSocketAddress().toString()
        + socket.getLocalPort() + "".replace('.', '_'));

    PROFILER.registerHookValue(profilerMetric + ".transmittedBytes", "Bytes transmitted to a network channel", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          public Object getValue() {
            return metricTransmittedBytes;
          }
        });
    PROFILER.registerHookValue(profilerMetric + ".receivedBytes", "Bytes received from a network channel", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          public Object getValue() {
            return metricReceivedBytes;
          }
        });
    PROFILER.registerHookValue(profilerMetric + ".flushes", "Number of times the network channel has been flushed",
        METRIC_TYPE.COUNTER, new OProfilerHookValue() {
          public Object getValue() {
            return metricFlushes;
          }
        });
  }

  @Override
  public String toString() {
    return socket != null ? socket.getRemoteSocketAddress().toString() : "Not connected";
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