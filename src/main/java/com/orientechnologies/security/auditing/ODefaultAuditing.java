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
package com.orientechnologies.security.auditing;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OAuditingOperation;
import com.orientechnologies.orient.core.security.OAuditingService;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Created by Enrico Risa on 10/04/15. */
public class ODefaultAuditing
    implements OAuditingService, ODatabaseLifecycleListener, ODistributedLifecycleListener {
  public static final String AUDITING_LOG_CLASSNAME = "OAuditingLog";

  private boolean enabled = true;
  private Integer globalRetentionDays = -1;
  private OrientDBInternal context;

  private Timer timer = new Timer();
  private OAuditingHook globalHook;

  private Map<String, OAuditingHook> hooks;

  private TimerTask retainTask;

  protected static final String DEFAULT_FILE_AUDITING_DB_CONFIG = "default-auditing-config.json";
  protected static final String FILE_AUDITING_DB_CONFIG = "auditing-config.json";

  private OAuditingDistribConfig distribConfig;

  private OSystemDBImporter systemDbImporter;

  private OSecuritySystem security;

  public static final String IMPORTER_FLAG = "AUDITING_IMPORTER";

  private class OAuditingDistribConfig extends OAuditingConfig {
    private boolean onNodeJoinedEnabled = false;
    private String onNodeJoinedMessage = "The node ${node} has joined";

    private boolean onNodeLeftEnabled = false;
    private String onNodeLeftMessage = "The node ${node} has left";

    public OAuditingDistribConfig(final ODocument cfg) {
      if (cfg.containsField("onNodeJoinedEnabled"))
        onNodeJoinedEnabled = cfg.field("onNodeJoinedEnabled");

      onNodeJoinedMessage = cfg.field("onNodeJoinedMessage");

      if (cfg.containsField("onNodeLeftEnabled"))
        onNodeLeftEnabled = cfg.field("onNodeLeftEnabled");

      onNodeLeftMessage = cfg.field("onNodeLeftMessage");
    }

    @Override
    public String formatMessage(final OAuditingOperation op, final String subject) {
      if (op == OAuditingOperation.NODEJOINED) {
        return resolveMessage(onNodeJoinedMessage, "node", subject);
      } else if (op == OAuditingOperation.NODELEFT) {
        return resolveMessage(onNodeLeftMessage, "node", subject);
      }

      return subject;
    }

    @Override
    public boolean isEnabled(OAuditingOperation op) {
      if (op == OAuditingOperation.NODEJOINED) {
        return onNodeJoinedEnabled;
      } else if (op == OAuditingOperation.NODELEFT) {
        return onNodeLeftEnabled;
      }

      return false;
    }
  }

  public ODefaultAuditing() {
    hooks = new ConcurrentHashMap<String, OAuditingHook>(20);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    // Don't audit system database events.
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME)) return;

    final OAuditingHook hook = defaultHook(iDatabase);
    hooks.put(iDatabase.getName(), hook);
    iDatabase.registerHook(hook);
    iDatabase.registerListener(hook);
  }

  private OAuditingHook defaultHook(final ODatabaseInternal iDatabase) {
    final File auditingFileConfig = getConfigFile(iDatabase.getName());
    String content = null;
    if (auditingFileConfig != null && auditingFileConfig.exists()) {
      content = getContent(auditingFileConfig);

    } else {
      final InputStream resourceAsStream =
          this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE_AUDITING_DB_CONFIG);
      try {
        if (resourceAsStream == null)
          OLogManager.instance().error(this, "defaultHook() resourceAsStream is null", null);

        content = getString(resourceAsStream);
        if (auditingFileConfig != null) {
          try {
            auditingFileConfig.getParentFile().mkdirs();
            auditingFileConfig.createNewFile();

            final FileOutputStream f = new FileOutputStream(auditingFileConfig);
            try {
              f.write(content.getBytes());
              f.flush();
            } finally {
              f.close();
            }
          } catch (IOException e) {
            content = "{}";
            OLogManager.instance().error(this, "Cannot save auditing file configuration", e);
          }
        }
      } finally {
        try {
          if (resourceAsStream != null) resourceAsStream.close();
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot read auditing file configuration", e);
        }
      }
    }
    final ODocument cfg = new ODocument().fromJSON(content, "noMap");
    return new OAuditingHook(cfg, security);
  }

  private String getContent(File auditingFileConfig) {
    FileInputStream f = null;
    String content = "";
    try {
      f = new FileInputStream(auditingFileConfig);
      final byte[] buffer = new byte[(int) auditingFileConfig.length()];
      f.read(buffer);

      content = new String(buffer);

    } catch (Exception e) {
      content = "{}";
      OLogManager.instance().error(this, "Cannot get auditing file configuration", e);
    } finally {
      if (f != null) {
        try {
          f.close();
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot get auditing file configuration", e);
        }
      }
    }
    return content;
  }

  public String getString(InputStream is) {

    try {
      int ch;
      final StringBuilder sb = new StringBuilder();
      while ((ch = is.read()) != -1) sb.append((char) ch);
      return sb.toString();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Cannot get default auditing file configuration", e);
      return "{}";
    }
  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {
    // Don't audit system database events.
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME)) return;

    // If the database has been opened by the auditing importer, do not hook it.
    if (iDatabase.getProperty(IMPORTER_FLAG) != null) return;

    OAuditingHook oAuditingHook = hooks.get(iDatabase.getName());
    if (oAuditingHook == null) {
      oAuditingHook = defaultHook(iDatabase);
      hooks.put(iDatabase.getName(), oAuditingHook);
    }
    iDatabase.registerHook(oAuditingHook);
    iDatabase.registerListener(oAuditingHook);
  }

  @Override
  public void onClose(ODatabaseInternal iDatabase) {
    final OAuditingHook oAuditingHook = hooks.get(iDatabase.getName());
    if (oAuditingHook != null) {
      iDatabase.unregisterHook(oAuditingHook);
      iDatabase.unregisterListener(oAuditingHook);
    }
  }

  @Override
  public void onDrop(ODatabaseInternal iDatabase) {
    onClose(iDatabase);

    final OAuditingHook oAuditingHook = hooks.get(iDatabase.getName());
    if (oAuditingHook != null) {
      oAuditingHook.shutdown(false);
    }

    File f = getConfigFile(iDatabase.getName());
    if (f != null && f.exists()) {
      OLogManager.instance()
          .info(this, "Removing Auditing config for db : %s", iDatabase.getName());
      f.delete();
    }
  }

  private File getConfigFile(String iDatabaseName) {
    return new File(
        security.getContext().getBasePath()
            + File.separator
            + iDatabaseName
            + File.separator
            + FILE_AUDITING_DB_CONFIG);
  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {
    final OAuditingHook oAuditingHook = hooks.get(iDatabase.getName());

    if (oAuditingHook != null) {
      oAuditingHook.onCreateClass(iClass);
    }
  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {
    final OAuditingHook oAuditingHook = hooks.get(iDatabase.getName());

    if (oAuditingHook != null) {
      oAuditingHook.onDropClass(iClass);
    }
  }

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {}

  protected void updateConfigOnDisk(final String iDatabaseName, final ODocument cfg)
      throws IOException {
    final File auditingFileConfig = getConfigFile(iDatabaseName);
    if (auditingFileConfig != null) {
      final FileOutputStream f = new FileOutputStream(auditingFileConfig);
      try {
        f.write(cfg.toJSON("prettyPrint=true").getBytes());
        f.flush();
      } finally {
        f.close();
      }
    }
  }

  //////
  // ODistributedLifecycleListener
  public boolean onNodeJoining(String iNode) {
    return true;
  }

  public void onNodeJoined(String iNode) {
    if (distribConfig != null && distribConfig.isEnabled(OAuditingOperation.NODEJOINED))
      log(
          OAuditingOperation.NODEJOINED,
          distribConfig.formatMessage(OAuditingOperation.NODEJOINED, iNode));
  }

  public void onNodeLeft(String iNode) {
    if (distribConfig != null && distribConfig.isEnabled(OAuditingOperation.NODELEFT))
      log(
          OAuditingOperation.NODELEFT,
          distribConfig.formatMessage(OAuditingOperation.NODELEFT, iNode));
  }

  public void onDatabaseChangeStatus(
      String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {}

  @Deprecated
  public static String getClusterName(final String dbName) {
    return dbName + "_auditing";
  }

  public static String getClassName(final String dbName) {
    return dbName + AUDITING_LOG_CLASSNAME;
  }

  //////
  // OAuditingService
  public void changeConfig(
      final OSecurityUser user, final String iDatabaseName, final ODocument cfg)
      throws IOException {

    // This should never happen, but just in case...
    // Don't audit system database events.
    if (iDatabaseName != null && iDatabaseName.equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      return;

    hooks.put(iDatabaseName, new OAuditingHook(cfg, security));

    updateConfigOnDisk(iDatabaseName, cfg);

    log(
        OAuditingOperation.CHANGEDCONFIG,
        user,
        String.format(
            "The auditing configuration for the database '%s' has been changed", iDatabaseName));
  }

  public ODocument getConfig(final String iDatabaseName) {
    return hooks.get(iDatabaseName).getConfiguration();
  }

  /** Primarily used for global logging events (e.g., NODEJOINED, NODELEFT). */
  public void log(final OAuditingOperation operation, final String message) {
    log(operation, null, null, message);
  }

  /** Primarily used for global logging events (e.g., NODEJOINED, NODELEFT). */
  public void log(final OAuditingOperation operation, OSecurityUser user, final String message) {
    log(operation, null, user, message);
  }

  /** Primarily used for global logging events (e.g., NODEJOINED, NODELEFT). */
  public void log(
      final OAuditingOperation operation,
      final String dbName,
      OSecurityUser user,
      final String message) {
    // If dbName is null, then we submit the log message to the global auditing hook.
    // Otherwise, we submit it to the hook associated with dbName.
    if (dbName != null) {
      final OAuditingHook oAuditingHook = hooks.get(dbName);

      if (oAuditingHook != null) {
        oAuditingHook.log(operation, dbName, user, message);
      } else { // Use the global hook.
        globalHook.log(operation, dbName, user, message);
      }
    } else { // Use the global hook.
      String userName = null;
      if (user != null) userName = user.getName();
      if (globalHook == null)
        OLogManager.instance()
            .error(
                this,
                "Default Auditing is disabled, cannot log: op=%s db='%s' user=%s message='%s'",
                null,
                operation,
                dbName,
                userName,
                message);
      else globalHook.log(operation, dbName, user, message);
    }
  }

  private void createClassIfNotExists() {
    final ODatabaseDocumentInternal currentDB =
        ODatabaseRecordThreadLocal.instance().getIfDefined();

    ODatabaseDocumentInternal sysdb = null;

    try {
      sysdb = context.getSystemDatabase().openSystemDatabase();

      OSchema schema = sysdb.getMetadata().getSchema();
      OClass cls = schema.getClass(AUDITING_LOG_CLASSNAME);

      if (cls == null) {
        cls = sysdb.getMetadata().getSchema().createClass(AUDITING_LOG_CLASSNAME);
        cls.createProperty("date", OType.DATETIME);
        cls.createProperty("user", OType.STRING);
        cls.createProperty("operation", OType.BYTE);
        cls.createProperty("record", OType.LINK);
        cls.createProperty("changes", OType.EMBEDDED);
        cls.createProperty("note", OType.STRING);
        cls.createProperty("database", OType.STRING);
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Creating auditing class exception", e);
    } finally {
      if (sysdb != null) sysdb.close();

      if (currentDB != null) ODatabaseRecordThreadLocal.instance().set(currentDB);
      else ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  //////
  // OAuditingService (OSecurityComponent)

  // Called once the Server is running.
  public void active() {
    createClassIfNotExists();

    globalHook = new OAuditingHook(security);

    retainTask =
        new TimerTask() {
          public void run() {
            retainLogs();
          }
        };

    long delay = 1000L;
    long period = 1000L * 60L * 60L * 24L;

    timer.scheduleAtFixedRate(retainTask, delay, period);

    Orient.instance().addDbLifecycleListener(this);
    if (context instanceof OServerAware) {
      if (((OServerAware) context).getDistributedManager() != null) {
        ((OServerAware) context).getDistributedManager().registerLifecycleListener(this);
      }
    }

    if (systemDbImporter != null && systemDbImporter.isEnabled()) {
      systemDbImporter.start();
    }
  }

  public void retainLogs() {

    if (globalRetentionDays > 0) {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date());
      c.add(Calendar.DATE, (-1) * globalRetentionDays);
      retainLogs(c.getTime());
    }
  }

  public void retainLogs(Date date) {
    long time = date.getTime();
    context
        .getSystemDatabase()
        .executeWithDB(
            (db -> {
              db.command("delete from OAuditingLog where date < ?", time).close();
              return null;
            }));
  }

  public void config(final ODocument jsonConfig, OSecuritySystem security) {
    context = security.getContext();
    this.security = security;
    try {
      if (jsonConfig.containsField("enabled")) {
        enabled = jsonConfig.field("enabled");
      }

      if (jsonConfig.containsField("retentionDays")) {
        globalRetentionDays = jsonConfig.field("retentionDays");
      }

      if (jsonConfig.containsField("distributed")) {
        ODocument distribDoc = jsonConfig.field("distributed");
        distribConfig = new OAuditingDistribConfig(distribDoc);
      }

      if (jsonConfig.containsField("systemImport")) {
        ODocument sysImport = jsonConfig.field("systemImport");

        systemDbImporter = new OSystemDBImporter(context, sysImport);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "config()", ex);
    }
  }

  // Called on removal of the component.
  public void dispose() {
    if (systemDbImporter != null && systemDbImporter.isEnabled()) {
      systemDbImporter.shutdown();
    }

    if (context instanceof OServerAware) {
      if (((OServerAware) context).getDistributedManager() != null) {
        ((OServerAware) context).getDistributedManager().unregisterLifecycleListener(this);
      }
    }

    Orient.instance().removeDbLifecycleListener(this);

    if (globalHook != null) {
      globalHook.shutdown(false);
      globalHook = null;
    }

    if (retainTask != null) {
      retainTask.cancel();
    }

    if (timer != null) {
      timer.cancel();
    }
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }
}
