/*
 * Copyright 2014 Charles Baptiste (cbaptiste--at--blacksparkcorp.com)
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
package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class OServerSSLSocketFactory extends OServerSocketFactory {

  public static final String PARAM_NETWORK_SSL_CLIENT_AUTH = "network.ssl.clientAuth";
  public static final String PARAM_NETWORK_SSL_KEYSTORE = "network.ssl.keyStore";
  public static final String PARAM_NETWORK_SSL_KEYSTORE_TYPE = "network.ssl.keyStoreType";
  public static final String PARAM_NETWORK_SSL_KEYSTORE_PASSWORD = "network.ssl.keyStorePassword";
  public static final String PARAM_NETWORK_SSL_TRUSTSTORE = "network.ssl.trustStore";
  public static final String PARAM_NETWORK_SSL_TRUSTSTORE_TYPE = "network.ssl.trustStoreType";
  public static final String PARAM_NETWORK_SSL_TRUSTSTORE_PASSWORD =
      "network.ssl.trustStorePassword";

  private SSLServerSocketFactory sslServerSocketFactory = null;

  private String keyStorePath = null;
  private File keyStoreFile = null;
  private String keyStorePassword = null;
  private String keyStoreType = KeyStore.getDefaultType();
  private String trustStorePath = null;
  private File trustStoreFile = null;
  private String trustStorePassword = null;
  private String trustStoreType = KeyStore.getDefaultType();
  private boolean clientAuth = false;

  public OServerSSLSocketFactory() {}

  @Override
  public void config(String name, final OServerParameterConfiguration[] iParameters) {

    super.config(name, iParameters);
    for (OServerParameterConfiguration param : iParameters) {
      if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_CLIENT_AUTH)) {
        clientAuth = Boolean.parseBoolean(param.value);
      } else if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_KEYSTORE)) {
        keyStorePath = param.value;
      } else if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_KEYSTORE_PASSWORD)) {
        keyStorePassword = param.value;
      } else if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_KEYSTORE_TYPE)) {
        keyStoreType = param.value;
      } else if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_TRUSTSTORE)) {
        trustStorePath = param.value;
      } else if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_TRUSTSTORE_PASSWORD)) {
        trustStorePassword = param.value;
      } else if (param.name.equalsIgnoreCase(PARAM_NETWORK_SSL_TRUSTSTORE_TYPE)) {
        trustStoreType = param.value;
      }
    }

    if (keyStorePath == null) {
      throw new OConfigurationException("Missing parameter " + PARAM_NETWORK_SSL_KEYSTORE);
    } else if (keyStorePassword == null) {
      throw new OConfigurationException("Missing parameter " + PARAM_NETWORK_SSL_KEYSTORE_PASSWORD);
    }

    keyStoreFile = new File(keyStorePath);
    if (!keyStoreFile.isAbsolute()) {
      keyStoreFile =
          new File(
              OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}"), keyStorePath);
    }

    if (trustStorePath != null) {
      trustStoreFile = new File(trustStorePath);
      if (!trustStoreFile.isAbsolute()) {
        trustStoreFile =
            new File(
                OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}"), trustStorePath);
      }
    }
  }

  private ServerSocket configureSocket(SSLServerSocket serverSocket) {

    serverSocket.setNeedClientAuth(clientAuth);

    return serverSocket;
  }

  private SSLServerSocketFactory getBackingFactory() {
    if (sslServerSocketFactory == null) {

      sslServerSocketFactory = getSSLContext().getServerSocketFactory();
    }
    return sslServerSocketFactory;
  }

  protected SSLContext getSSLContext() {

    try {
      SSLContext context = SSLContext.getInstance("TLS");

      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      char[] keyStorePass = keyStorePassword.toCharArray();
      OServerSSLCertificateManager oServerSSLCertificateManager =
          OServerSSLCertificateManager.getInstance(this, keyStore, keyStoreFile, keyStorePass);
      oServerSSLCertificateManager.loadKeyStoreForSSLSocket();
      kmf.init(keyStore, keyStorePass);

      TrustManagerFactory tmf = null;
      if (trustStoreFile != null) {
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        char[] trustStorePass = trustStorePassword.toCharArray();
        oServerSSLCertificateManager.loadTrustStoreForSSLSocket(
            trustStore, trustStoreFile, trustStorePass);
        tmf.init(trustStore);
      }

      context.init(kmf.getKeyManagers(), (tmf == null ? null : tmf.getTrustManagers()), null);

      return context;

    } catch (Exception e) {
      throw OException.wrapException(
          new OConfigurationException("Failed to create SSL context"), e);
    }
  }

  @Override
  public ServerSocket createServerSocket(int port) throws IOException {
    return configureSocket((SSLServerSocket) getBackingFactory().createServerSocket(port));
  }

  @Override
  public ServerSocket createServerSocket(int port, int backlog) throws IOException {
    return configureSocket((SSLServerSocket) getBackingFactory().createServerSocket(port, backlog));
  }

  @Override
  public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress)
      throws IOException {
    return configureSocket(
        (SSLServerSocket) getBackingFactory().createServerSocket(port, backlog, ifAddress));
  }
}
