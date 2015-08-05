/*
 * Copyright 2010-2013 Orient Technologies LTD
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 */

package com.orientechnologies.workbench;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.workbench.event.*;
import com.orientechnologies.workbench.hooks.OEventHook;
import com.orientechnologies.workbench.http.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class OWorkbenchPlugin extends OServerPluginAbstract {

  public enum STATUS {
    OFFLINE, ONLINE, UNAUTHORIZED, LICENSE_EXPIRED, LICENSE_INVALID, NO_AGENT
  }

  public static final String                        VERSION                           = OConstants.ORIENT_VERSION;
  static final String                               SYSTEM_CONFIG                     = "system.config";

  public static final String                        CLASS_CLUSTER                     = "Cluster";
  public static final String                        CLASS_SERVER                      = "Server";
  public static final String                        CLASS_LOG                         = "Log";
  public static final String                        CLASS_EVENT                       = "Event";

  public static final String                        CLASS_EVENT_WHEN                  = "EventWhen";
  public static final String                        CLASS_EVENT_WHAT                  = "EventWhat";

  // public static final String CLASS_SCHEDULER_WHEN = "SchedulerWhen";
  public static final String                        CLASS_LOG_WHEN                    = "LogWhen";
  private static final String                       CLASS_METRICS_WHEN                = "MetricsWhen";

  public static final String                        CLASS_HTTP_WHAT                   = "HttpWhat";
  public static final String                        CLASS_MAIL_WHAT                   = "MailWhat";
  private static final String                       CLASS_FUNCTION_WHAT               = "FunctionWhat";

  public static final String                        CLASS_SNAPSHOT                    = "Snapshot";
  public static final String                        CLASS_METRIC                      = "Metric";
  public static final String                        CLASS_COUNTER                     = "Counter";
  public static final String                        CLASS_CHRONO                      = "Chrono";
  public static final String                        CLASS_STATISTIC                   = "Statistic";
  public static final String                        CLASS_INFORMATION                 = "Information";
  public static final String                        CLASS_DICTIONARY                  = "Dictionary";

  public static final String                        CLASS_USER_CONFIGURATION          = "UserConfiguration";
  public static final String                        CLASS_MAIL_PROFILE                = "OMailProfile";
  public static final String                        CLASS_DELETE_METRIC_CONFIG        = "DeleteMetricConfiguration";
  public static final String                        CLASS_DELETE_NOTIFICATIONS_CONFIG = "NotificationsConfiguration";
  private static final String                       CLASS_UPDATE_CONFIG               = "updateConfiguration";
  public static final String                        CLASS_METRIC_CONFIG               = "MetricConfig";
  private static final String                       CLASS_PROXY_CONFIG                = "ProxyConfiguration";
  private static final String                       CLASS_MESSAGE                     = "Message";

  private OServer                                   serverInstance;
  private long                                      updateTimer;
  private long                                      purgeTimer                        = 1000 * 60 * 30;
  private String                                    dbName                            = "monitor";
  private String                                    dbUser                            = "wb";
  private String                                    dbPassword                        = "OrientDB_KILLS_Neo4J!";
  private ODatabaseDocumentTx                       db;
  Map<String, OMonitoredServer>                     servers                           = new HashMap<String, OMonitoredServer>();
  private Map<String, OMonitoredCluster>            clusters                          = new HashMap<String, OMonitoredCluster>();
  Map<Integer, Map<Integer, Set<OMonitoredServer>>> keyMap;
  Map<String, OPair<String, METRIC_TYPE>>           dictionary;
  private Set<OServerConfigurationListener>         listeners                         = new HashSet<OServerConfigurationListener>();
  private ConcurrentHashMap<String, Boolean>        metricsEnabled                    = new ConcurrentHashMap<String, Boolean>();
  public String                                     version;
  private OWorkbenchUpdateTask                      updater;
  private OWorkbenchMessageTask                     messageTask;
  private OWorkbenchHazelcastTask                   hazelcastTask;

  @Override
  public void config(OServer iServer, OServerParameterConfiguration[] iParams) {
    serverInstance = iServer;

    updateTimer = OIOUtils.getTimeAsMillisecs("10s");
    dbName = "plocal:" + OServerMain.server().getDatabaseDirectory() + dbName;
    for (OServerParameterConfiguration p : iParams) {
      if (p.name.equals("version"))
        version = p.value;
    }
  }

  @Override
  public String getName() {
    return "monitor";
  }

  @Override
  public void startup() {
    setDb(new ODatabaseDocumentTx(dbName));
    if (getDb().exists())
      loadConfiguration();
    else
      createConfiguration();

    updateDictionary();

    getDb().registerHook(new OEventHook());

    registerExecutors(getDb());
    registerCommands();
    Orient.instance().getTimer().schedule(new OWorkbenchRealtimeTask(this), 5000, 5000);
    Orient.instance().getTimer().schedule(new OWorkbenchTask(this), updateTimer, updateTimer);
    Orient.instance().getTimer().schedule(new OWorkbenchPurgeTask(this), purgeTimer, purgeTimer);
    hazelcastTask = new OWorkbenchHazelcastTask(this);
    Orient.instance().getTimer().schedule(hazelcastTask, updateTimer, updateTimer);
    messageTask = new OWorkbenchMessageTask(this);
    Orient.instance().getTimer().schedule(messageTask, 600000, 600000);
    updater = new OWorkbenchUpdateTask(this);
    Orient.instance().getTimer().schedule(updater, 600000, 600000);

    System.out.printf("\n\n************************************************");
    System.out.printf("\n*   ORIENTDB WORKBENCH -  ENTERPRISE EDITION   *");
    System.out.printf("\n*                                              *");
    System.out.printf("\n* Copyrights (c) 2014 Orient Technologies LTD  *");
    System.out.printf("\n************************************************");
    System.out.printf("\n* Version...: %-32s *", VERSION);
    System.out.printf("\n************************************************\n");

    for (OServerNetworkListener l : serverInstance.getNetworkListeners()) {
      if (l.getProtocolType().equals(ONetworkProtocolHttpDb.class)) {
        System.out
            .printf(
                "\nTo open the Web Console open your browser to the URL: http://%s and use 'admin' as user and password to log in, unless you already changed it.\n\n",
                l.getListeningAddress(true));
        break;
      }
    }

  }

  private void registerExecutors(ODatabaseDocumentTx database) {
    OEventController.getInstance().register(new OEventMetricMailExecutor(database));
    OEventController.getInstance().register(new OEventLogMailExecutor(database));
    OEventController.getInstance().register(new OEventLogFunctionExecutor(database));
    OEventController.getInstance().register(new OEventMetricFunctionExecutor(database));
    OEventController.getInstance().register(new OEventLogHttpExecutor(database));
    OEventController.getInstance().register(new OEventMetricHttpExecutor(database));
  }

  public void pauseClusterInspection() {
    hazelcastTask.pause();
  }

  public void resumeClusterInspection() {
    hazelcastTask.resume();
  }

  private void registerCommands() {
    final OServerNetworkListener listener = serverInstance.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandGetRealtimeMetrics());
    listener.registerStatelessCommand(new OServerCommandQueryPassThrough());
    listener.registerStatelessCommand(new OServerCommandAuthenticateSingleDatabase());
    listener.registerStatelessCommand(new OServerCommandGetLoggedUserInfo());
    listener.registerStatelessCommand(new OServerCommandGetMonitoredServers());
    listener.registerStatelessCommand(new OServerCommandGetExplainCommand());
    listener.registerStatelessCommand(new OServerCommandGetConnectionsCommand());
    listener.registerStatelessCommand(new OServerCommandDeleteRealtimeMetrics());
    listener.registerStatelessCommand(new OServerCommandGetServerLog());
    listener.registerStatelessCommand(new OServerCommandGetServerConfiguration());
    listener.registerStatelessCommand(new OServerCommandPurgeMetric());
    listener.registerStatelessCommand(new OServerCommandMgrServer());
    listener.registerStatelessCommand(new OServerCommandNotifyChangedMetric());
    listener.registerStatelessCommand(new OServerCommandMessageExecute());
    listener.registerStatelessCommand(new OServerCommandDistributedManager());
  }

  private void unregisterCommands() {
    final OServerNetworkListener listener = serverInstance.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.unregisterStatelessCommand(OServerCommandGetRealtimeMetrics.class);
    listener.unregisterStatelessCommand(OServerCommandQueryPassThrough.class);
    listener.unregisterStatelessCommand(OServerCommandAuthenticateSingleDatabase.class);
    listener.unregisterStatelessCommand(OServerCommandGetLoggedUserInfo.class);
    listener.unregisterStatelessCommand(OServerCommandGetMonitoredServers.class);
    listener.unregisterStatelessCommand(OServerCommandGetExplainCommand.class);
    listener.unregisterStatelessCommand(OServerCommandGetConnectionsCommand.class);
    listener.unregisterStatelessCommand(OServerCommandDeleteRealtimeMetrics.class);
    listener.unregisterStatelessCommand(OServerCommandGetServerLog.class);
    listener.unregisterStatelessCommand(OServerCommandGetServerConfiguration.class);
    listener.unregisterStatelessCommand(OServerCommandPurgeMetric.class);
    listener.unregisterStatelessCommand(OServerCommandMgrServer.class);
    listener.unregisterStatelessCommand(OServerCommandNotifyChangedMetric.class);
    listener.unregisterStatelessCommand(OServerCommandMessageExecute.class);
    listener.unregisterStatelessCommand(OServerCommandDistributedManager.class);
  }

  public OMonitoredServer getMonitoredServer(final String iServer) {
    return servers.get(iServer);
  }

  public void removeMonitoredServer(final String name) {
    servers.remove(name);
  }

  public Set<Entry<String, OMonitoredServer>> getMonitoredServers() {
    return Collections.unmodifiableSet(servers.entrySet());
  }

  public Map<Integer, Map<Integer, Set<OMonitoredServer>>> getKeyMap() {
    return keyMap;
  }

  public void updateActiveServerList() {
    Map<Integer, Map<Integer, Set<OMonitoredServer>>> keyMap = new HashMap<Integer, Map<Integer, Set<OMonitoredServer>>>();
    Map<String, OMonitoredServer> tmpServers = new HashMap<String, OMonitoredServer>();
    final List<ODocument> enabledServers = getDb().query(new OSQLSynchQuery<Object>("select from Server where enabled = true"));
    for (ODocument s : enabledServers) {
      final String serverName = s.field("name");

      OMonitoredServer serverCfg = servers.get(serverName);
      if (serverCfg == null) {
        serverCfg = new OMonitoredServer(this, s);
      }
      Map<String, Object> cfg = s.field("configuration");
      // if (cfg != null) {
      // String license = (String) cfg.get("license");
      // int idC = OL.getClientId(license);
      // int idS = OL.getServerId(license);
      // Map<Integer, Set<OMonitoredServer>> serv = keyMap.get(idC);
      // if (serv == null) {
      // serv = new HashMap<Integer, Set<OMonitoredServer>>();
      // }
      // Set<OMonitoredServer> mSer = serv.get(idS);
      // if (mSer == null) {
      // mSer = new HashSet<OMonitoredServer>();
      // }
      // mSer.add(serverCfg);
      // serv.put(idS, mSer);
      // keyMap.put(idC, serv);
      // }
      tmpServers.put(serverName, serverCfg);
    }
    this.keyMap = keyMap;
    this.servers = tmpServers;
  }

  public Collection<OServerConfigurationListener> getListeners() {
    return Collections.unmodifiableCollection(listeners);
  }

  public OWorkbenchPlugin addListeners(final OServerConfigurationListener iListener) {
    listeners.add(iListener);
    return this;
  }

  protected void loadConfiguration() {
    getDb().open(dbUser, dbPassword);

    ODatabaseRecordThreadLocal.INSTANCE.set(getDb());
    // LOAD THE SERVERS CONFIGURATION
    updateActiveServerList();

    // UPDATE LAST CONNECTION FOR EACH SERVERS
    final List<ODocument> snapshotDates = getDb().query(
        new OSQLSynchQuery<Object>(
            "select server.name as serverName, max(dateTo) as date from Snapshot where server.enabled = true group by server"));

    for (ODocument snapshot : snapshotDates) {
      final String serverName = snapshot.field("serverName");

      final OMonitoredServer serverCfg = servers.get(serverName);
      if (serverCfg != null)
        serverCfg.setLastConnection((Date) snapshot.field("date"));
    }
    OLogManager.instance().info(this, "Loading server configuration (%d)...", servers.size());
    for (Entry<String, OMonitoredServer> serverEntry : servers.entrySet()) {
      OLogManager.instance().info(this, "* server [%s] updated to: %s", serverEntry.getKey(),
          serverEntry.getValue().getLastConnection());
    }
  }

  protected void createConfiguration() {
    OLogManager.instance().info(this, "Creating %s database...", dbName);
    getDb().create();

    getDb().set(ODatabase.ATTRIBUTES.CUSTOM, "strictSql=false");

    final OSchema schema = getDb().getMetadata().getSchema();

    final OClass server = schema.createClass(CLASS_SERVER);
    server.createProperty("name", OType.STRING);
    server.createProperty("url", OType.STRING);
    server.createProperty("user", OType.STRING);
    server.createProperty("password", OType.STRING);

    final OClass cluster = schema.createClass(CLASS_CLUSTER);
    cluster.createProperty("name", OType.STRING);
    cluster.createProperty("password", OType.STRING);
    cluster.createProperty("port", OType.INTEGER);
    cluster.createProperty("portIncrement", OType.BOOLEAN);

    final OClass snapshot = schema.createClass(CLASS_SNAPSHOT);
    snapshot.createProperty("server", OType.LINK, server);
    snapshot.createProperty("dateFrom", OType.DATETIME);
    snapshot.createProperty("dateTo", OType.DATETIME);

    final OClass metric = schema.createClass(CLASS_METRIC);
    OProperty prop = metric.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);
    metric.createProperty("snapshot", OType.LINK, snapshot);

    final OClass log = schema.createClass(CLASS_LOG);
    log.createProperty("date", OType.DATETIME);
    log.createProperty("level", OType.STRING);
    log.createProperty("server", OType.LINK, server);
    log.createProperty("message", OType.STRING);
    log.createProperty("levelDescription", OType.STRING);

    final OClass chrono = schema.createClass(CLASS_CHRONO).setSuperClass(metric);
    chrono.createProperty("entries", OType.LONG);
    chrono.createProperty("last", OType.LONG);
    chrono.createProperty("min", OType.LONG);
    chrono.createProperty("max", OType.LONG);
    chrono.createProperty("average", OType.LONG);
    chrono.createProperty("total", OType.LONG);

    final OClass counter = schema.createClass(CLASS_COUNTER).setSuperClass(metric);
    counter.createProperty("value", OType.LONG);

    final OClass statistics = schema.createClass(CLASS_STATISTIC).setSuperClass(metric);
    statistics.createProperty("value", OType.STRING);

    final OClass information = schema.createClass(CLASS_INFORMATION).setSuperClass(metric);
    information.createProperty("value", OType.STRING);

    final OClass eventWhat = schema.createClass(CLASS_EVENT_WHAT);
    final OClass eventWhen = schema.createClass(CLASS_EVENT_WHEN);

    final OClass events = schema.createClass(CLASS_EVENT);
    events.createProperty("name", OType.STRING);
    events.createProperty("when", OType.EMBEDDED, eventWhen);
    events.createProperty("what", OType.EMBEDDED, eventWhat);

    final OClass logEvent = schema.createClass(CLASS_LOG_WHEN);
    logEvent.setSuperClass(eventWhen);
    logEvent.createProperty("type", OType.STRING);
    logEvent.createProperty("info", OType.STRING);
    logEvent.createProperty("server", OType.EMBEDDED, server);
    logEvent.createProperty("alertValue", OType.STRING);

    final OClass metrics = schema.createClass(CLASS_METRICS_WHEN);
    metrics.setSuperClass(eventWhen);
    metrics.createProperty("name", OType.STRING);
    metrics.createProperty("operator", OType.STRING);// Greater, Less
    metrics.createProperty("parameter", OType.STRING);
    metrics.createProperty("value", OType.DOUBLE);

    final OClass http = schema.createClass(CLASS_HTTP_WHAT);
    http.setSuperClass(eventWhat);
    http.createProperty("method", OType.STRING);
    http.createProperty("url", OType.STRING);
    // http.createProperty("port", OType.INTEGER);
    http.createProperty("body", OType.STRING);
    http.createProperty("proxy", OType.STRING);

    final OClass mail = schema.createClass(CLASS_MAIL_WHAT);
    mail.setSuperClass(eventWhat);
    mail.createProperty("body", OType.STRING);
    mail.createProperty("subject", OType.STRING);
    mail.createProperty("fromAddress", OType.STRING);
    mail.createProperty("toAddress", OType.STRING);
    mail.createProperty("cc", OType.STRING);
    mail.createProperty("bcc", OType.STRING);

    final OClass function = schema.createClass(CLASS_FUNCTION_WHAT);
    function.setSuperClass(eventWhat);
    function.createProperty("code", OType.STRING);
    function.createProperty("idempotent", OType.BOOLEAN);
    function.createProperty("language", OType.STRING);
    function.createProperty("name", OType.STRING);
    function.createProperty("parameters", OType.EMBEDDEDLIST, OType.STRING);

    final OClass metricConfig = schema.createClass(CLASS_METRIC_CONFIG);
    metricConfig.createProperty("name", OType.STRING);
    metricConfig.createProperty("server", OType.LINK, server);

    final OClass userConfig = schema.createClass(CLASS_USER_CONFIGURATION);
    final OClass ouser = schema.getClass(OUser.class);

    final OClass profile = schema.createClass(CLASS_MAIL_PROFILE);
    final OClass deleteMetricConfiguration = schema.createClass(CLASS_DELETE_METRIC_CONFIG);
    deleteMetricConfiguration.createProperty("hours", OType.INTEGER);

    final OClass notificationsConfiguration = schema.createClass(CLASS_DELETE_NOTIFICATIONS_CONFIG);
    notificationsConfiguration.createProperty("hours", OType.INTEGER);

    final OClass proxyConfiguration = schema.createClass(CLASS_PROXY_CONFIG);
    notificationsConfiguration.createProperty("proxyIp", OType.STRING);
    notificationsConfiguration.createProperty("proxyPort", OType.INTEGER);

    profile.createProperty("user", OType.STRING);
    profile.createProperty("password", OType.STRING);
    profile.createProperty("port", OType.INTEGER);
    profile.createProperty("enabled", OType.BOOLEAN);
    profile.createProperty("auth", OType.BOOLEAN);
    profile.createProperty("starttlsEnable", OType.BOOLEAN);
    profile.createProperty("dateFormat", OType.STRING);
    profile.createProperty("host", OType.STRING);

    final OClass updateConfiguration = schema.createClass(CLASS_UPDATE_CONFIG);
    updateConfiguration.createProperty("receiveNews", OType.BOOLEAN);
    updateConfiguration.createProperty("hours", OType.INTEGER);

    userConfig.createProperty("user", OType.LINK, ouser);
    userConfig.createProperty("mailProfile", OType.EMBEDDED, profile);
    userConfig.createProperty("deleteMetricConfiguration", OType.EMBEDDED, deleteMetricConfiguration);
    userConfig.createProperty("notificationsConfiguration", OType.EMBEDDED, notificationsConfiguration);
    userConfig.createProperty("proxyConfiguration", OType.EMBEDDED, proxyConfiguration);
    userConfig.createProperty("orientdbSite", OType.STRING);
    userConfig.createProperty("metrics", OType.LINKLIST, metricConfig);
    userConfig.createProperty("updateConfiguration", OType.EMBEDDED, updateConfiguration);
    final OClass message = schema.createClass(CLASS_MESSAGE);

    message.createProperty("message", OType.STRING);
    message.createProperty("status", OType.STRING);
    message.createProperty("read", OType.BOOLEAN);
    message.createProperty("type", OType.STRING);
    message.createProperty("subject", OType.STRING);
    message.createProperty("payload", OType.STRING);

    try {
      // OVERWRITE ADMIN PASSWORD
      OUser user = db.getMetadata().getSecurity().getUser("admin");
      user.setName(dbUser);
      user.setPassword(dbPassword);
      user.save();

      // CREATE ADMIN WITH WRITER ROLE
      db.getMetadata().getSecurity().createUser("admin", "admin", "admin");

      // REOPEN THE DATABASE WITH NEW CREDENTIALS
      db.close();
      db.open(dbUser, dbPassword);

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on creation of admin user", e);
    }
  }

  @Override
  public void shutdown() {
    unregisterCommands();
  }

  public OWorkbenchUpdateTask getUpdater() {
    return updater;
  }

  public OWorkbenchMessageTask getMessageTask() {
    return messageTask;
  }

  public OServer getServerInstance() {
    return serverInstance;
  }

  protected void updateDictionary() {
    final OSchema schema = getDb().getMetadata().getSchema();

    if (!schema.existsClass(CLASS_DICTIONARY)) {
      final OClass dictionary = schema.createClass(CLASS_DICTIONARY);
      final OProperty name = dictionary.createProperty("name", OType.STRING);
      name.createIndex(INDEX_TYPE.UNIQUE_HASH_INDEX);
    }

    if (dictionary == null)
      dictionary = Orient.instance().getProfiler().getMetadata();

    for (Entry<String, OPair<String, METRIC_TYPE>> entry : dictionary.entrySet()) {
      try {
        final String key = entry.getKey();
        final OPair<String, METRIC_TYPE> value = entry.getValue();

        final ODocument doc = new ODocument(CLASS_DICTIONARY);
        doc.field("name", key);
        doc.field("description", value.getKey());
        doc.field("type", value.getValue());
        doc.field("enabled", Boolean.TRUE);
        doc.save();

      } catch (Exception e) {
        // IGNORE DUPLICATES
      }
    }
  }

  public ConcurrentHashMap<String, Boolean> getMetricsEnabled() {
    return metricsEnabled;
  }

  public Map<String, OPair<String, METRIC_TYPE>> getDictionary() {
    return dictionary;
  }

  public ODatabaseDocumentTx getDb() {
    return db;
  }

  public void setDb(ODatabaseDocumentTx db) {
    this.db = db;
  }

  public void addCluster(OMonitoredCluster cluster) {
    clusters.put(cluster.getName(), cluster);
  }

  public boolean hasCluster(String name) {
    return clusters.get(name) != null;
  }

  public Map<String, OMonitoredCluster> getClusters() {
    return clusters;
  }

  public Collection<OMonitoredCluster> getClustersList() {
    return clusters.values();
  }

  public OMonitoredCluster getClusterByName(String cluster) {
    return clusters.get(cluster);
  }

  public Collection<OMonitoredServer> getServersByClusterName(String cluster) {
    List<OMonitoredServer> srvs = new ArrayList<OMonitoredServer>();
    for (OMonitoredServer s : servers.values()) {
      ODocument doc = s.getConfiguration().field("cluster");
      if (doc != null && cluster.equals(doc.field("name"))) {
        srvs.add(s);
      }
    }
    return srvs;
  }

  public void removeMonitoredCluster(String cluster) {
    clusters.remove(cluster);
  }
}
