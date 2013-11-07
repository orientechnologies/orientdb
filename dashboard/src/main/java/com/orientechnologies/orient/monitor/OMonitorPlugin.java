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

package com.orientechnologies.orient.monitor;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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
import com.orientechnologies.orient.monitor.event.OEventController;
import com.orientechnologies.orient.monitor.event.OEventLogFunctionExecutor;
import com.orientechnologies.orient.monitor.event.OEventLogHttpExecutor;
import com.orientechnologies.orient.monitor.event.OEventLogMailExecutor;
import com.orientechnologies.orient.monitor.event.OEventMetricFunctionExecutor;
import com.orientechnologies.orient.monitor.event.OEventMetricHttpExecutor;
import com.orientechnologies.orient.monitor.event.OEventMetricMailExecutor;
import com.orientechnologies.orient.monitor.hooks.OEventHook;
import com.orientechnologies.orient.monitor.http.OServerCommandDeleteServer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;

public class OMonitorPlugin extends OServerHandlerAbstract {
	public enum LOG_LEVEL {
		DEBUG, INFO, CONFIG, WARN, ERROR
	}

	public enum STATUS {
		OFFLINE, ONLINE, UNAUTHORIZED, PROFILEROFF, LICENSE_EXPIRED, LICENSE_INVALID
	}

	public static final String												VERSION														= OConstants.ORIENT_VERSION;
	static final String																SYSTEM_CONFIG											= "system.config";

	public static final String												CLASS_SERVER											= "Server";
	public static final String												CLASS_LOG													= "Log";
	public static final String												CLASS_EVENT												= "Event";

	public static final String												CLASS_EVENT_WHEN									= "EventWhen";
	public static final String												CLASS_EVENT_WHAT									= "EventWhat";

	// public static final String CLASS_SCHEDULER_WHEN = "SchedulerWhen";
	public static final String												CLASS_LOG_WHEN										= "LogWhen";
	private static final String												CLASS_METRICS_WHEN								= "MetricsWhen";

	public static final String												CLASS_HTTP_WHAT										= "HttpWhat";
	public static final String												CLASS_MAIL_WHAT										= "MailWhat";
	private static final String												CLASS_FUNCTION_WHAT								= "FunctionWhat";

	public static final String												CLASS_SNAPSHOT										= "Snapshot";
	public static final String												CLASS_METRIC											= "Metric";
	public static final String												CLASS_COUNTER											= "Counter";
	public static final String												CLASS_CHRONO											= "Chrono";
	public static final String												CLASS_STATISTIC										= "Statistic";
	public static final String												CLASS_INFORMATION									= "Information";
	public static final String												CLASS_DICTIONARY									= "Dictionary";

	public static final String												CLASS_USER_CONFIGURATION					= "UserConfiguration";
	public static final String												CLASS_MAIL_PROFILE								= "OMailProfile";
	public static final String												CLASS_DELETE_METRIC_CONFIG				= "DeleteMetricConfiguration";
	public static final String												CLASS_DELETE_NOTIFICATIONS_CONFIG	= "NotificationsConfiguration";

	public static final String												CLASS_METRIC_CONFIG								= "MetricConfig";
	private static final String												CLASS_PROXY_CONFIG								= "ProxyConfiguration";
	private static final String												CLASS_MESSAGE								= "Message";

	private OServer																		serverInstance;
	private long																			updateTimer;
	private long																			purgeTimer												= 1000 * 60 * 30;
	private String																		dbName														= "monitor";
	private String																		dbUser														= "admin";
	private String																		dbPassword												= "admin";
	private ODatabaseDocumentTx												db;
	Map<String, OMonitoredServer>											servers														= new HashMap<String, OMonitoredServer>();
	Map<Integer, Map<Integer, Set<OMonitoredServer>>>	keyMap;
	Map<String, OPair<String, METRIC_TYPE>>						dictionary;
	private Set<OServerConfigurationListener>					listeners													= new HashSet<OServerConfigurationListener>();

	@Override
	public void config(OServer iServer, OServerParameterConfiguration[] iParams) {
		serverInstance = iServer;
		OLogManager.instance().info(this, "Installing OrientDB Enterprise MONITOR v.%s...", VERSION);

		for (OServerParameterConfiguration param : iParams) {
			if (param.name.equalsIgnoreCase("updateTimer"))
				updateTimer = OIOUtils.getTimeAsMillisecs(param.value);
			else if (param.name.equalsIgnoreCase("dbName")) {
				dbName = param.value;
				dbName = "plocal:" + OServerMain.server().getDatabaseDirectory() + dbName;
			} else if (param.name.equalsIgnoreCase("dbUser"))
				dbUser = param.value;
			else if (param.name.equalsIgnoreCase("dbPassword"))
				dbPassword = param.value;
		}
	}

	@Override
	public String getName() {
		return "monitor";
	}

	@Override
	public void startup() {
		setDb(new ODatabaseDocumentTx(dbName));
		OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
		OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(false);
		if (getDb().exists())
			loadConfiguration();
		else
			createConfiguration();

		updateDictionary();

		getDb().registerHook(new OEventHook());

		registerExecutors(getDb());
		registerCommand();
		Orient.instance().getTimer().schedule(new OMonitorTask(this), updateTimer, updateTimer);

		// Orient.instance()
		// .getTimer()
		// .schedule(new OMonitorPurgeTask(this), 1000*60*30, 1000*60*30);
		//
		Orient.instance().getTimer().schedule(new OMonitorPurgeTask(this), purgeTimer, purgeTimer);
		Orient.instance().getTimer().schedule(new OMonitorMessageTask(this), 5000, 5000);
	}

	private void registerExecutors(ODatabaseDocumentTx database) {
		OEventController.getInstance().register(new OEventMetricMailExecutor(database));
		OEventController.getInstance().register(new OEventLogMailExecutor(database));
		OEventController.getInstance().register(new OEventLogFunctionExecutor(database));
		OEventController.getInstance().register(new OEventMetricFunctionExecutor(database));
		OEventController.getInstance().register(new OEventLogHttpExecutor(database));
		OEventController.getInstance().register(new OEventMetricHttpExecutor(database));
	}

	private void registerCommand() {
		final OServerNetworkListener listener = serverInstance.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
		if (listener == null)
			throw new OConfigurationException("HTTP listener not found");
	}

	public OMonitoredServer getMonitoredServer(final String iServer) {
		return servers.get(iServer);
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
			if (cfg != null) {
				String license = (String) cfg.get("license");
				int idC = OL.getClientId(license);
				int idS = OL.getServerId(license);
				Map<Integer, Set<OMonitoredServer>> serv = keyMap.get(idC);
				if (serv == null) {
					serv = new HashMap<Integer, Set<OMonitoredServer>>();
				}
				Set<OMonitoredServer> mSer = serv.get(idS);
				if (mSer == null) {
					mSer = new HashSet<OMonitoredServer>();
				}
				mSer.add(serverCfg);
				serv.put(idS, mSer);
				keyMap.put(idC, serv);
			}
			tmpServers.put(serverName, serverCfg);
		}
		this.keyMap = keyMap;
		this.servers = tmpServers;
	}

	public Collection<OServerConfigurationListener> getListeners() {
		return Collections.unmodifiableCollection(listeners);
	}

	public OMonitorPlugin addListeners(final OServerConfigurationListener iListener) {
		listeners.add(iListener);
		return this;
	}

	protected void loadConfiguration() {
		getDb().open(dbUser, dbPassword);

		// LOAD THE SERVERS CONFIGURATION
		updateActiveServerList();

		// UPDATE LAST CONNECTION FOR EACH SERVERS
		final List<ODocument> snapshotDates = getDb().query(
				new OSQLSynchQuery<Object>("select server.name as serverName, max(dateTo) as date from Snapshot where server.enabled = true group by server"));

		for (ODocument snapshot : snapshotDates) {
			final String serverName = snapshot.field("serverName");

			final OMonitoredServer serverCfg = servers.get(serverName);
			if (serverCfg != null)
				serverCfg.setLastConnection((Date) snapshot.field("date"));
		}
		OLogManager.instance().info(this, "MONITOR loading server configuration (%d)...", servers.size());
		for (Entry<String, OMonitoredServer> serverEntry : servers.entrySet()) {
			OLogManager.instance().info(this, "MONITOR * server [%s] updated to: %s", serverEntry.getKey(), serverEntry.getValue().getLastConnection());
		}
	}

	protected void createConfiguration() {
		OLogManager.instance().info(this, "MONITOR creating %s database...", dbName);
		getDb().create();

		final OSchema schema = getDb().getMetadata().getSchema();

		final OClass server = schema.createClass(CLASS_SERVER);
		server.createProperty("name", OType.STRING);
		server.createProperty("url", OType.STRING);
		server.createProperty("user", OType.STRING);
		server.createProperty("password", OType.STRING);

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
		function.createProperty("parameters", OType.EMBEDDED, OType.STRING);

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

		userConfig.createProperty("user", OType.LINK, ouser);
		userConfig.createProperty("mailProfile", OType.EMBEDDED, profile);
		userConfig.createProperty("deleteMetricConfiguration", OType.EMBEDDED, deleteMetricConfiguration);
		userConfig.createProperty("notificationsConfiguration", OType.EMBEDDED, notificationsConfiguration);
		userConfig.createProperty("proxyConfiguration", OType.EMBEDDED, proxyConfiguration);
    userConfig.createProperty("orientdbSite", OType.STRING);
		userConfig.createProperty("metrics", OType.LINKLIST, metricConfig);
		final OClass message = schema.createClass(CLASS_MESSAGE);
		
		message.createProperty("message", OType.STRING);
		message.createProperty("read", OType.BOOLEAN);
		
	}

	@Override
	public void shutdown() {
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

	public Map<String, OPair<String, METRIC_TYPE>> getDictionary() {
		return dictionary;
	}

	public ODatabaseDocumentTx getDb() {
		return db;
	}

	public void setDb(ODatabaseDocumentTx db) {
		this.db = db;
	}
}
