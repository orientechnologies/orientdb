package com.orientechnologies.orient.client.remote;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_FETCH_HOST_LIST;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemote.CONNECTION_STRATEGY;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

public class ORemoteURLs {

  private static final int DEFAULT_PORT = 2424;
  private static final int DEFAULT_SSL_PORT = 2434;

  private final List<String> serverURLs = new ArrayList<String>();
  private List<String> initialServerURLs;
  private int nextServerToConnect;

  public ORemoteURLs(String[] hosts, OContextConfiguration config) {
    for (String host : hosts) {
      addHost(host, config);
    }
    this.initialServerURLs = new ArrayList<String>(serverURLs);
    this.nextServerToConnect = 0;
  }

  public synchronized void remove(String serverUrl) {
    serverURLs.remove(serverUrl);
    this.nextServerToConnect = 0;
  }

  public synchronized List<String> getUrls() {
    return Collections.unmodifiableList(serverURLs);
  }

  public synchronized String removeAndGet(String url) {
    remove(url);
    OLogManager.instance().debug(this, "Updated server list: %s...", serverURLs);

    if (!serverURLs.isEmpty()) return serverURLs.get(0);
    else return null;
  }

  public synchronized void addAll(List<String> toAdd, OContextConfiguration clientConfiguration) {
    if (toAdd.size() > 0) {
      serverURLs.clear();
      this.nextServerToConnect = 0;
      for (String host : toAdd) addHost(host, clientConfiguration);
    }
  }

  /** Registers the remote server with port. */
  protected String addHost(String host, OContextConfiguration clientConfiguration) {

    if (host.contains("/")) host = host.substring(0, host.indexOf("/"));

    // REGISTER THE REMOTE SERVER+PORT
    if (!host.contains(":")) {
      if (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL)) {
        host += ":" + getDefaultSSLPort();
      } else {
        host += ":" + getDefaultPort();
      }
    } else if (host.split(":").length < 2 || host.split(":")[1].trim().length() == 0) {
      if (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL)) {
        host += getDefaultSSLPort();
      } else {
        host += getDefaultPort();
      }
    }

    if (!serverURLs.contains(host)) {
      serverURLs.add(host);
      OLogManager.instance().debug(this, "Registered the new available server '%s'", host);
    }

    return host;
  }

  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }

  protected int getDefaultSSLPort() {
    return DEFAULT_SSL_PORT;
  }

  private static List<String> parseAddressesFromUrl(String url) {
    List<String> addresses = new ArrayList<>();
    int dbPos = url.indexOf('/');
    if (dbPos == -1) {
      // SHORT FORM
      addresses.add(url);
    } else {
      for (String host : url.substring(0, dbPos).split(OStorageRemote.ADDRESS_SEPARATOR)) {
        addresses.add(host);
      }
    }
    return addresses;
  }

  public synchronized String parseServerUrls(
      String url, OContextConfiguration contextConfiguration) {
    int dbPos = url.indexOf('/');
    String name;
    if (dbPos == -1) {
      // SHORT FORM
      name = url;
    } else {
      name = url.substring(url.lastIndexOf("/") + 1);
    }
    String lastHost = null;
    List<String> hosts = parseAddressesFromUrl(url);
    for (String host : hosts) {
      lastHost = host;
      addHost(host, contextConfiguration);
    }

    if (serverURLs.size() == 1
        && contextConfiguration.getValueAsBoolean(
            OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED)) {
      List<String> toAdd = fetchHostsFromDns(lastHost, contextConfiguration);
      serverURLs.addAll(toAdd);
    }
    this.initialServerURLs = new ArrayList<String>(serverURLs);
    return name;
  }

  public synchronized void reloadOriginalURLs() {
    this.serverURLs.clear();
    this.serverURLs.addAll(this.initialServerURLs);
  }

  private List<String> fetchHostsFromDns(
      final String primaryServer, OContextConfiguration contextConfiguration) {
    OLogManager.instance()
        .debug(
            this,
            "Retrieving URLs from DNS '%s' (timeout=%d)...",
            primaryServer,
            contextConfiguration.getValueAsInteger(
                OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT));

    List<String> toAdd = new ArrayList<>();
    try {
      final Hashtable<String, String> env = new Hashtable<String, String>();
      env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
      env.put(
          "com.sun.jndi.ldap.connect.timeout",
          contextConfiguration.getValueAsString(
              OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT));

      final DirContext ictx = new InitialDirContext(env);
      final String hostName =
          !primaryServer.contains(":")
              ? primaryServer
              : primaryServer.substring(0, primaryServer.indexOf(":"));
      final Attributes attrs = ictx.getAttributes(hostName, new String[] {"TXT"});
      final Attribute attr = attrs.get("TXT");
      if (attr != null) {
        for (int i = 0; i < attr.size(); ++i) {
          String configuration = (String) attr.get(i);
          if (configuration.startsWith("\""))
            configuration = configuration.substring(1, configuration.length() - 1);
          if (configuration != null) {
            final String[] parts = configuration.split(" ");
            for (String part : parts) {
              if (part.startsWith("s=")) {
                toAdd.add(part.substring("s=".length()));
              }
            }
          }
        }
      }
    } catch (NamingException ignore) {
    }
    return toAdd;
  }

  private synchronized String getNextConnectUrl(
      OStorageRemoteSession session, OContextConfiguration contextConfiguration) {
    if (serverURLs.isEmpty()) {
      reloadOriginalURLs();
      if (serverURLs.isEmpty())
        throw new OStorageException(
            "Cannot create a connection to remote server because url list is empty");
    }

    this.nextServerToConnect++;
    if (this.nextServerToConnect >= serverURLs.size())
      // RESET INDEX
      this.nextServerToConnect = 0;

    final String serverURL = serverURLs.get(this.nextServerToConnect);
    if (session != null) {
      session.serverURLIndex = this.nextServerToConnect;
      session.currentUrl = serverURL;
    }

    return serverURL;
  }

  public synchronized String getServerURFromList(
      boolean iNextAvailable,
      OStorageRemoteSession session,
      OContextConfiguration contextConfiguration) {
    if (session != null && session.getCurrentUrl() != null && !iNextAvailable) {
      return session.getCurrentUrl();
    }
    if (serverURLs.isEmpty()) {
      reloadOriginalURLs();
      if (serverURLs.isEmpty())
        throw new OStorageException(
            "Cannot create a connection to remote server because url list is empty");
    }

    // GET CURRENT THREAD INDEX
    int serverURLIndex;
    if (session != null) serverURLIndex = session.serverURLIndex;
    else serverURLIndex = 0;

    if (iNextAvailable) serverURLIndex++;

    if (serverURLIndex < 0 || serverURLIndex >= serverURLs.size())
      // RESET INDEX
      serverURLIndex = 0;

    final String serverURL = serverURLs.get(serverURLIndex);

    if (session != null) {
      session.serverURLIndex = serverURLIndex;
      session.currentUrl = serverURL;
    }

    return serverURL;
  }

  public synchronized String getNextAvailableServerURL(
      boolean iIsConnectOperation,
      OStorageRemoteSession session,
      OContextConfiguration contextConfiguration,
      CONNECTION_STRATEGY strategy) {
    String url = null;
    if (session.isStickToSession()) {
      strategy = CONNECTION_STRATEGY.STICKY;
    }
    switch (strategy) {
      case STICKY:
        url = session.getServerUrl();
        if (url == null) url = getServerURFromList(false, session, contextConfiguration);
        break;

      case ROUND_ROBIN_CONNECT:
        if (iIsConnectOperation || session.getServerUrl() == null) {
          url = getNextConnectUrl(session, contextConfiguration);
        } else {
          url = session.getServerUrl();
        }
        OLogManager.instance()
            .debug(
                this,
                "ROUND_ROBIN_CONNECT: Next remote operation will be executed on server: %s (isConnectOperation=%s)",
                url,
                iIsConnectOperation);
        break;

      case ROUND_ROBIN_REQUEST:
        url = getServerURFromList(true, session, contextConfiguration);
        OLogManager.instance()
            .debug(
                this,
                "ROUND_ROBIN_REQUEST: Next remote operation will be executed on server: %s (isConnectOperation=%s)",
                url,
                iIsConnectOperation);
        break;

      default:
        throw new OConfigurationException("Connection mode " + strategy + " is not supported");
    }

    return url;
  }

  public synchronized void updateDistributedNodes(
      List<String> hosts, OContextConfiguration clientConfiguration) {
    if (!clientConfiguration.getValueAsBoolean(CLIENT_CONNECTION_FETCH_HOST_LIST)) {
      List<String> definedHosts = initialServerURLs;
      for (String host : definedHosts) {
        addHost(host, clientConfiguration);
      }
      return;
    }
    // UPDATE IT
    for (String host : hosts) {
      addHost(host, clientConfiguration);
    }
  }
}
