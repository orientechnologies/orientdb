/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.security.auditing;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OAuditingOperation;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.security.OAuditingService;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 10/04/15.
 */
public class ODefaultAuditing implements OAuditingService, ODatabaseLifecycleListener, ODistributedLifecycleListener {
  public static final String         AUDITING_LOG_CLASSNAME          = "OAuditingLog";

  private boolean                    enabled                         = true;
  private OServer                    server;

  private OAuditingHook              globalHook;

  private Map<String, OAuditingHook> hooks;

  protected static final String      DEFAULT_FILE_AUDITING_DB_CONFIG = "default-auditing-config.json";
  protected static final String      FILE_AUDITING_DB_CONFIG         = "auditing-config.json";

  private OAuditingDistribConfig     distribConfig;

  private OSystemDBImporter          systemDbImporter;
  private static final String        IMPORTER_FLAG                   = "AUDITING_IMPORTER";

  private class OAuditingDistribConfig extends OAuditingConfig {
    private boolean onNodeJoinedEnabled = false;
    private String  onNodeJoinedMessage = "The node ${node} has joined";

    private boolean onNodeLeftEnabled   = false;
    private String  onNodeLeftMessage   = "The node ${node} has left";

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
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      return;

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
      final InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE_AUDITING_DB_CONFIG);

      if (resourceAsStream == null)
        OLogManager.instance().error(this, "defaultHook() resourceAsStream is null");

      content = getString(resourceAsStream);
      if (auditingFileConfig != null) {
        try {
          auditingFileConfig.getParentFile().mkdirs();
          auditingFileConfig.createNewFile();

          final FileOutputStream f = new FileOutputStream(auditingFileConfig);
          f.write(content.getBytes());
          f.flush();
        } catch (IOException e) {
          content = "{}";
          OLogManager.instance().error(this, "Cannot save auditing file configuration", e);
        }
      }
    }
    final ODocument cfg = new ODocument().fromJSON(content, "noMap");
    return new OAuditingHook(cfg, server);
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
    }
    return content;
  }

  public String getString(InputStream is) {

    try {
      int ch;
      final StringBuilder sb = new StringBuilder();
      while ((ch = is.read()) != -1)
        sb.append((char) ch);
      return sb.toString();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Cannot get default auditing file configuration", e);
      return "{}";
    }

  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {
    // Don't audit system database events.
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      return;

    // If the database has been opened by the auditing importer, do not hook it.
    if (iDatabase.getProperty(IMPORTER_FLAG) != null)
      return;

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
      OLogManager.instance().info(this, "Removing Auditing config for db : %s", iDatabase.getName());
      f.delete();
    }
  }

  private File getConfigFile(String iDatabaseName) {
    OStorage storage = Orient.instance().getStorage(iDatabaseName);

    if (storage instanceof OLocalPaginatedStorage) {
      return new File(((OLocalPaginatedStorage) storage).getStoragePath() + File.separator + FILE_AUDITING_DB_CONFIG);
    }

    return null;
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
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {
  }

  protected void updateConfigOnDisk(final String iDatabaseName, final ODocument cfg) throws IOException {
    final File auditingFileConfig = getConfigFile(iDatabaseName);
    if (auditingFileConfig != null) {
      final FileOutputStream f = new FileOutputStream(auditingFileConfig);
      f.write(cfg.toJSON("prettyPrint=true").getBytes());
      f.flush();
    }
  }

  //////
  // ODistributedLifecycleListener
  public boolean onNodeJoining(String iNode) {
    return true;
  }

  public void onNodeJoined(String iNode) {
    if (distribConfig != null && distribConfig.isEnabled(OAuditingOperation.NODEJOINED))
      log(OAuditingOperation.NODEJOINED, distribConfig.formatMessage(OAuditingOperation.NODEJOINED, iNode));
  }

  public void onNodeLeft(String iNode) {
    if (distribConfig != null && distribConfig.isEnabled(OAuditingOperation.NODELEFT))
      log(OAuditingOperation.NODELEFT, distribConfig.formatMessage(OAuditingOperation.NODELEFT, iNode));
  }

  public void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {

  }

  @Deprecated
  public static String getClusterName(final String dbName) {
    return dbName + "_auditing";
  }

  public static String getClassName(final String dbName) {
    return dbName + AUDITING_LOG_CLASSNAME;
  }

  //////
  // OAuditingService
  public void changeConfig(final String iDatabaseName, final ODocument cfg) throws IOException {

    // This should never happen, but just in case...
    // Don't audit system database events.
    if (iDatabaseName != null && iDatabaseName.equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      return;

    hooks.put(iDatabaseName, new OAuditingHook(cfg, server));

    updateConfigOnDisk(iDatabaseName, cfg);
  }

  public ODocument getConfig(final String iDatabaseName) {
    return hooks.get(iDatabaseName).getConfiguration();
  }

  /**
   * Primarily used for global logging events (e.g., NODEJOINED, NODELEFT).
   */
  public void log(final OAuditingOperation operation, final String message) {
    log(operation, null, null, message);
  }

  /**
   * Primarily used for global logging events (e.g., NODEJOINED, NODELEFT).
   */
  public void log(final OAuditingOperation operation, final String username, final String message) {
    log(operation, null, username, message);
  }

  /**
   * Primarily used for global logging events (e.g., NODEJOINED, NODELEFT).
   */
  public void log(final OAuditingOperation operation, final String dbName, final String username, final String message) {
    // If dbName is null, then we submit the log message to the global auditing hook.
    // Otherwise, we submit it to the hook associated with dbName.
    if (dbName != null) {
      final OAuditingHook oAuditingHook = hooks.get(dbName);

      if (oAuditingHook != null) {
        oAuditingHook.log(operation, dbName, username, message);
      } else { // Use the global hook.
        globalHook.log(operation, dbName, username, message);
      }
    } else { // Use the global hook.
      globalHook.log(operation, dbName, username, message);
    }
  }

  private void createClassIfNotExists() {
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    try {
      ODatabaseDocumentInternal sysdb = server.getSystemDatabase().openSystemDatabase();

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
      OLogManager.instance().error(this, "Creating auditing class exception: %s", e.getMessage());
    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.INSTANCE.set(currentDB);
      else
        ODatabaseRecordThreadLocal.INSTANCE.remove();
    }
  }

  // Used by OSystemDBImporter to set the IMPORTER_FLAG property in order to prevent
  // the import actions from being logged.
  public static ODatabaseDocumentTx openImporterDatabase(final OServer server, final String dbName) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + server.getDatabaseDirectory() + dbName);
    db.setProperty(IMPORTER_FLAG, true);
    db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);

    try {
      db.open("OSystemDBImporter", "nopasswordneeded");
    } catch (Exception e) {
      db = null;
      OLogManager.instance().error(null, "openImporterDatabase() Cannot open database '%s'", e, dbName);
    }

    return db;
  }

  //////
  // OAuditingService (OSecurityComponent)

  // Called once the Server is running.
  public void active() {
    createClassIfNotExists();

    globalHook = new OAuditingHook(server);

    Orient.instance().addDbLifecycleListener(this);

    if (server.getDistributedManager() != null) {
      server.getDistributedManager().registerLifecycleListener(this);
    }

    if (systemDbImporter != null && systemDbImporter.isEnabled()) {
      systemDbImporter.start();
    }
  }

  public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig) {
    server = oServer;

    try {
      if (jsonConfig.containsField("enabled")) {
        enabled = jsonConfig.field("enabled");
      }

      if (jsonConfig.containsField("distributed")) {
        ODocument distribDoc = jsonConfig.field("distributed");
        distribConfig = new OAuditingDistribConfig(distribDoc);
      }

      if (jsonConfig.containsField("systemImport")) {
        ODocument sysImport = jsonConfig.field("systemImport");

        systemDbImporter = new OSystemDBImporter(server, sysImport);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "config() Exception: %s", ex.getMessage());
    }
  }

  // Called on removal of the component.
  public void dispose() {
    if (systemDbImporter != null && systemDbImporter.isEnabled()) {
      systemDbImporter.shutdown();
    }

    if (server.getDistributedManager() != null) {
      server.getDistributedManager().unregisterLifecycleListener(this);
    }

    Orient.instance().removeDbLifecycleListener(this);

    if (globalHook != null) {
      globalHook.shutdown(false);
      globalHook = null;
    }
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }
}
