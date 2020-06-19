/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.cloudbees.syslog.sender;

import com.cloudbees.syslog.SyslogMessage;
import com.cloudbees.syslog.util.CachingReference;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;

/**
 * Syslog message sender over UDP.
 *
 * <p>TODO optimize performances recycling the byte arrays. Note: {@link
 * java.io.ByteArrayOutputStream} can be subclassed to access to the underlying {@code byte[]}.
 *
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public class UdpSyslogMessageSender extends AbstractSyslogMessageSender {
  /**
   * {@link java.net.InetAddress InetAddress} of the remote Syslog Server.
   *
   * <p>The {@code InetAddress} is refreshed regularly to handle DNS changes (default {@link
   * #DEFAULT_INET_ADDRESS_TTL_IN_MILLIS})
   *
   * <p>Default value: {@link #DEFAULT_SYSLOG_HOST}
   */
  protected CachingReference<InetAddress> syslogServerHostnameReference;
  /**
   * Listen port of the remote Syslog server.
   *
   * <p>Default: {@link #DEFAULT_SYSLOG_PORT}
   */
  protected int syslogServerPort = DEFAULT_SYSLOG_PORT;

  private DatagramSocket datagramSocket;

  public UdpSyslogMessageSender() {
    try {
      setSyslogServerHostname(DEFAULT_SYSLOG_HOST);
      datagramSocket = new DatagramSocket();
    } catch (IOException e) {
      throw new IllegalStateException("Exception initializing datagramSocket", e);
    }
  }

  /**
   * Send the given {@link com.cloudbees.syslog.SyslogMessage} over UDP.
   *
   * @param message the message to send
   * @throws IOException
   */
  @Override
  public void sendMessage(SyslogMessage message) throws IOException {
    sendCounter.incrementAndGet();
    long nanosBefore = System.nanoTime();

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Writer out = new OutputStreamWriter(baos, UTF_8);

      message.toSyslogMessage(messageFormat, out);
      out.flush();

      if (logger.isLoggable(Level.FINEST)) {
        logger.finest("Send syslog message " + new String(baos.toByteArray(), UTF_8));
      }
      byte[] bytes = baos.toByteArray();

      DatagramPacket packet =
          new DatagramPacket(
              bytes, bytes.length, syslogServerHostnameReference.get(), syslogServerPort);
      datagramSocket.send(packet);
    } catch (IOException e) {
      sendErrorCounter.incrementAndGet();
      throw e;
    } catch (RuntimeException e) {
      sendErrorCounter.incrementAndGet();
      throw e;
    } finally {
      sendDurationInNanosCounter.addAndGet(System.nanoTime() - nanosBefore);
    }
  }

  public void setSyslogServerHostname(final String syslogServerHostname) {
    this.syslogServerHostnameReference =
        new CachingReference<InetAddress>(DEFAULT_INET_ADDRESS_TTL_IN_NANOS) {
          @Override
          protected InetAddress newObject() {
            try {
              return InetAddress.getByName(syslogServerHostname);
            } catch (UnknownHostException e) {
              throw new IllegalStateException(e);
            }
          }
        };
  }

  public void setSyslogServerPort(int syslogServerPort) {
    this.syslogServerPort = syslogServerPort;
  }

  public String getSyslogServerHostname() {
    InetAddress inetAddress = syslogServerHostnameReference.get();
    return inetAddress == null ? null : inetAddress.getHostName();
  }

  public int getSyslogServerPort() {
    return syslogServerPort;
  }

  @Override
  public String toString() {
    return getClass().getName()
        + "{"
        + "syslogServerHostname='"
        + this.getSyslogServerHostname()
        + '\''
        + ", syslogServerPort='"
        + this.getSyslogServerPort()
        + '\''
        + ", defaultAppName='"
        + defaultAppName
        + '\''
        + ", defaultFacility="
        + defaultFacility
        + ", defaultMessageHostname='"
        + defaultMessageHostname
        + '\''
        + ", defaultSeverity="
        + defaultSeverity
        + ", messageFormat="
        + messageFormat
        + ", sendCounter="
        + sendCounter
        + ", sendDurationInNanosCounter="
        + sendDurationInNanosCounter
        + ", sendErrorCounter="
        + sendErrorCounter
        + '}';
  }
}
