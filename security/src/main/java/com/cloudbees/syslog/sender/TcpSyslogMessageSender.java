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
import com.cloudbees.syslog.util.IoUtils;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.net.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * See <a href="http://tools.ietf.org/html/rfc6587">RFC 6587 - Transmission of Syslog Messages over
 * TCP</a>
 *
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public class TcpSyslogMessageSender extends AbstractSyslogMessageSender {
  public static final int SETTING_SOCKET_CONNECT_TIMEOUT_IN_MILLIS_DEFAULT_VALUE = 500;
  public static final int SETTING_MAX_RETRY = 2;

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

  private Socket socket;
  private Writer writer;
  private int socketConnectTimeoutInMillis = SETTING_SOCKET_CONNECT_TIMEOUT_IN_MILLIS_DEFAULT_VALUE;
  private boolean ssl;
  /** Number of retries to send a message before throwing an exception. */
  private int maxRetryCount = SETTING_MAX_RETRY;
  /** Number of exceptions trying to send message. */
  protected final AtomicInteger trySendErrorCounter = new AtomicInteger();

  @Override
  public synchronized void sendMessage(SyslogMessage message) throws IOException {
    sendCounter.incrementAndGet();
    long nanosBefore = System.nanoTime();

    try {
      Exception lastException = null;
      for (int i = 0; i <= maxRetryCount; i++) {
        try {
          if (logger.isLoggable(Level.FINEST)) {
            logger.finest("Send syslog message " + message.toSyslogMessage(messageFormat));
          }
          ensureSyslogServerConnection();
          message.toSyslogMessage(messageFormat, writer);
          // use the CR LF non transparent framing as described in "3.4.2.  Non-Transparent-Framing"
          writer.write("\r\n");
          writer.flush();
          return;
        } catch (IOException e) {
          lastException = e;
          IoUtils.closeQuietly(socket, writer);
          trySendErrorCounter.incrementAndGet();
        } catch (RuntimeException e) {
          lastException = e;
          IoUtils.closeQuietly(socket, writer);
          trySendErrorCounter.incrementAndGet();
        }
      }
      if (lastException != null) {
        sendErrorCounter.incrementAndGet();
        if (lastException instanceof IOException) {
          throw (IOException) lastException;
        } else if (lastException instanceof RuntimeException) {
          throw (RuntimeException) lastException;
        }
      }
    } finally {
      sendDurationInNanosCounter.addAndGet(System.nanoTime() - nanosBefore);
    }
  }

  private synchronized void ensureSyslogServerConnection() throws IOException {
    InetAddress inetAddress = syslogServerHostnameReference.get();
    if (socket != null && !Objects.equals(socket.getInetAddress(), inetAddress)) {
      logger.info(
          "InetAddress of the Syslog Server have changed, create a new connection. "
              + "Before="
              + socket.getInetAddress()
              + ", new="
              + inetAddress);
      IoUtils.closeQuietly(socket, writer);
      writer = null;
      socket = null;
    }
    boolean socketIsValid;
    try {
      socketIsValid =
          socket != null
              && socket.isConnected()
              && socket.isBound()
              && !socket.isClosed()
              && !socket.isInputShutdown()
              && !socket.isOutputShutdown();
    } catch (Exception e) {
      socketIsValid = false;
    }
    if (!socketIsValid) {
      writer = null;
      try {
        if (ssl) {
          socket = SSLSocketFactory.getDefault().createSocket();
        } else {
          socket = SocketFactory.getDefault().createSocket();
        }
        socket.setKeepAlive(true);
        socket.connect(
            new InetSocketAddress(inetAddress, syslogServerPort), socketConnectTimeoutInMillis);

        if (socket instanceof SSLSocket && logger.isLoggable(Level.FINER)) {
          try {
            SSLSocket sslSocket = (SSLSocket) socket;
            SSLSession session = sslSocket.getSession();
            logger.finer("The Certificates used by peer");
            for (Certificate certificate : session.getPeerCertificates()) {
              if (certificate instanceof X509Certificate) {
                X509Certificate x509Certificate = (X509Certificate) certificate;
                logger.finer("" + x509Certificate.getSubjectDN());
              } else {
                logger.finer("" + certificate);
              }
            }
            logger.finer("Peer host is " + session.getPeerHost());
            logger.finer("Cipher is " + session.getCipherSuite());
            logger.finer("Protocol is " + session.getProtocol());
            logger.finer("ID is " + new BigInteger(session.getId()));
            logger.finer("Session created in " + session.getCreationTime());
            logger.finer("Session accessed in " + session.getLastAccessedTime());
          } catch (Exception e) {
            logger.warn("Exception dumping debug info for " + socket, e);
          }
        }
      } catch (IOException e) {
        ConnectException ce =
            new ConnectException("Exception connecting to " + inetAddress + ":" + syslogServerPort);
        ce.initCause(e);
        throw ce;
      }
    }
    if (writer == null) {
      writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), UTF_8));
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

  public boolean isSsl() {
    return ssl;
  }

  public void setSsl(boolean ssl) {
    this.ssl = ssl;
  }

  public int getSocketConnectTimeoutInMillis() {
    return socketConnectTimeoutInMillis;
  }

  public int getMaxRetryCount() {
    return maxRetryCount;
  }

  public int getTrySendErrorCounter() {
    return trySendErrorCounter.get();
  }

  public void setSocketConnectTimeoutInMillis(int socketConnectTimeoutInMillis) {
    this.socketConnectTimeoutInMillis = socketConnectTimeoutInMillis;
  }

  public void setMaxRetryCount(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
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
        + ", ssl="
        + ssl
        + ", maxRetryCount="
        + maxRetryCount
        + ", socketConnectTimeoutInMillis="
        + socketConnectTimeoutInMillis
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
        + ", trySendErrorCounter="
        + trySendErrorCounter
        + '}';
  }
}
