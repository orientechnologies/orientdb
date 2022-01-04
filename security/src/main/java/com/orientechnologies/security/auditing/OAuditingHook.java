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

import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.security.OAuditingOperation;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hook to audit database access.
 *
 * @author Luca Garulli
 */
public class OAuditingHook extends ORecordHookAbstract implements ODatabaseListener {
  private final Map<String, OAuditingClassConfig> classes =
      new HashMap<String, OAuditingClassConfig>(20);
  private final OAuditingLoggingThread auditingThread;

  private Map<ODatabaseDocument, List<ODocument>> operations =
      new ConcurrentHashMap<ODatabaseDocument, List<ODocument>>();
  private volatile LinkedBlockingQueue<ODocument> auditingQueue;
  private Set<OAuditingCommandConfig> commands = new HashSet<OAuditingCommandConfig>();
  private boolean onGlobalCreate;
  private boolean onGlobalRead;
  private boolean onGlobalUpdate;
  private boolean onGlobalDelete;
  private OAuditingClassConfig defaultConfig = new OAuditingClassConfig();
  private OAuditingSchemaConfig schemaConfig;
  private ODocument iConfiguration;

  private static class OAuditingCommandConfig {
    public String regex;
    public String message;

    public OAuditingCommandConfig(final ODocument cfg) {
      regex = cfg.field("regex");
      message = cfg.field("message");
    }
  }

  private static class OAuditingClassConfig {
    public boolean polymorphic = true;
    public boolean onCreateEnabled = false;
    public String onCreateMessage;
    public boolean onReadEnabled = false;
    public String onReadMessage;
    public boolean onUpdateEnabled = false;
    public String onUpdateMessage;
    public boolean onUpdateChanges = true;
    public boolean onDeleteEnabled = false;
    public String onDeleteMessage;

    public OAuditingClassConfig() {}

    public OAuditingClassConfig(final ODocument cfg) {
      if (cfg.containsField("polymorphic")) polymorphic = cfg.field("polymorphic");

      // CREATE
      if (cfg.containsField("onCreateEnabled")) onCreateEnabled = cfg.field("onCreateEnabled");
      if (cfg.containsField("onCreateMessage")) onCreateMessage = cfg.field("onCreateMessage");

      // READ
      if (cfg.containsField("onReadEnabled")) onReadEnabled = cfg.field("onReadEnabled");
      if (cfg.containsField("onReadMessage")) onReadMessage = cfg.field("onReadMessage");

      // UPDATE
      if (cfg.containsField("onUpdateEnabled")) onUpdateEnabled = cfg.field("onUpdateEnabled");
      if (cfg.containsField("onUpdateMessage")) onUpdateMessage = cfg.field("onUpdateMessage");
      if (cfg.containsField("onUpdateChanges")) onUpdateChanges = cfg.field("onUpdateChanges");

      // DELETE
      if (cfg.containsField("onDeleteEnabled")) onDeleteEnabled = cfg.field("onDeleteEnabled");
      if (cfg.containsField("onDeleteMessage")) onDeleteMessage = cfg.field("onDeleteMessage");
    }
  }

  // Handles the auditing-config "schema" configuration.
  private class OAuditingSchemaConfig extends OAuditingConfig {
    private boolean onCreateClassEnabled = false;
    private String onCreateClassMessage;

    private boolean onDropClassEnabled = false;
    private String onDropClassMessage;

    public OAuditingSchemaConfig(final ODocument cfg) {
      if (cfg.containsField("onCreateClassEnabled"))
        onCreateClassEnabled = cfg.field("onCreateClassEnabled");

      onCreateClassMessage = cfg.field("onCreateClassMessage");

      if (cfg.containsField("onDropClassEnabled"))
        onDropClassEnabled = cfg.field("onDropClassEnabled");

      onDropClassMessage = cfg.field("onDropClassMessage");
    }

    @Override
    public String formatMessage(final OAuditingOperation op, final String subject) {
      if (op == OAuditingOperation.CREATEDCLASS) {
        return resolveMessage(onCreateClassMessage, "class", subject);
      } else if (op == OAuditingOperation.DROPPEDCLASS) {
        return resolveMessage(onDropClassMessage, "class", subject);
      }

      return subject;
    }

    @Override
    public boolean isEnabled(OAuditingOperation op) {
      if (op == OAuditingOperation.CREATEDCLASS) {
        return onCreateClassEnabled;
      } else if (op == OAuditingOperation.DROPPEDCLASS) {
        return onDropClassEnabled;
      }

      return false;
    }
  }

  //// OAuditingHook
  public OAuditingHook(final String iConfiguration) {
    this(new ODocument().fromJSON(iConfiguration, "noMap"), null);
  }

  public OAuditingHook(final String iConfiguration, final OSecuritySystem system) {
    this(new ODocument().fromJSON(iConfiguration, "noMap"), system);
  }

  public OAuditingHook(final ODocument iConfiguration) {
    this(iConfiguration, null);
  }

  public OAuditingHook(final ODocument iConfiguration, final OSecuritySystem system) {
    this.iConfiguration = iConfiguration;

    onGlobalCreate = onGlobalRead = onGlobalUpdate = onGlobalDelete = false;

    final ODocument classesCfg = iConfiguration.field("classes");
    if (classesCfg != null) {
      for (String c : classesCfg.fieldNames()) {
        final OAuditingClassConfig cfg = new OAuditingClassConfig((ODocument) classesCfg.field(c));
        if (c.equals("*")) defaultConfig = cfg;
        else classes.put(c, cfg);

        if (cfg.onCreateEnabled) onGlobalCreate = true;
        if (cfg.onReadEnabled) onGlobalRead = true;
        if (cfg.onUpdateEnabled) onGlobalUpdate = true;
        if (cfg.onDeleteEnabled) onGlobalDelete = true;
      }
    }

    final Iterable<ODocument> commandCfg = iConfiguration.field("commands");

    if (commandCfg != null) {

      for (ODocument cfg : commandCfg) {
        commands.add(new OAuditingCommandConfig(cfg));
      }
    }

    final ODocument schemaCfgDoc = iConfiguration.field("schema");
    if (schemaCfgDoc != null) {
      schemaConfig = new OAuditingSchemaConfig(schemaCfgDoc);
    }

    auditingQueue = new LinkedBlockingQueue<ODocument>();
    auditingThread =
        new OAuditingLoggingThread(
            ODatabaseRecordThreadLocal.instance().get().getName(),
            auditingQueue,
            system.getContext(),
            system);

    auditingThread.start();
  }

  public OAuditingHook(final OSecuritySystem server) {
    auditingQueue = new LinkedBlockingQueue<ODocument>();
    auditingThread =
        new OAuditingLoggingThread(
            OSystemDatabase.SYSTEM_DB_NAME, auditingQueue, server.getContext(), server);

    auditingThread.start();
  }

  @Override
  public void onCreate(ODatabase iDatabase) {}

  @Override
  public void onDelete(ODatabase iDatabase) {}

  @Override
  public void onOpen(ODatabase iDatabase) {}

  @Override
  public void onBeforeTxBegin(ODatabase iDatabase) {}

  @Override
  public void onBeforeTxRollback(ODatabase iDatabase) {}

  @Override
  public void onAfterTxRollback(ODatabase iDatabase) {

    synchronized (operations) {
      operations.remove(iDatabase);
    }
  }

  @Override
  public void onBeforeTxCommit(ODatabase iDatabase) {}

  @Override
  public void onAfterTxCommit(ODatabase iDatabase) {

    List<ODocument> oDocuments = null;

    synchronized (operations) {
      oDocuments = operations.remove(iDatabase);
    }
    if (oDocuments != null) {
      for (ODocument oDocument : oDocuments) {
        auditingQueue.offer(oDocument);
      }
    }
  }

  @Override
  public void onClose(ODatabase iDatabase) {}

  @Override
  public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {}

  @Override
  public void onAfterCommand(
      OCommandRequestText iCommand, OCommandExecutor executor, Object result) {
    logCommand(iCommand.getText());
  }

  @Override
  public boolean onCorruptionRepairDatabase(
      ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
    return false;
  }

  public ODocument getConfiguration() {
    return iConfiguration;
  }

  @Override
  public void onRecordAfterCreate(final ORecord iRecord) {
    if (!onGlobalCreate) return;

    log(OAuditingOperation.CREATED, iRecord);
  }

  @Override
  public void onRecordAfterRead(final ORecord iRecord) {
    if (!onGlobalRead) return;

    log(OAuditingOperation.LOADED, iRecord);
  }

  @Override
  public void onRecordAfterUpdate(final ORecord iRecord) {

    if (iRecord instanceof ODocument) {
      ODocument doc = (ODocument) iRecord;
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(db, doc);

      if (clazz.isOuser() && Arrays.asList(doc.getDirtyFields()).contains("password")) {
        String name = doc.getProperty("name");
        String message = String.format("The password for user '%s' has been changed", name);
        log(OAuditingOperation.CHANGED_PWD, db.getName(), db.getUser(), message);
      }
    }
    if (!onGlobalUpdate) return;

    log(OAuditingOperation.UPDATED, iRecord);
  }

  @Override
  public void onRecordAfterDelete(final ORecord iRecord) {
    if (!onGlobalDelete) return;

    log(OAuditingOperation.DELETED, iRecord);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
  }

  protected void logCommand(final String command) {
    if (auditingQueue == null) return;

    for (OAuditingCommandConfig cfg : commands) {
      if (command.matches(cfg.regex)) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();

        final ODocument doc =
            createLogDocument(
                OAuditingOperation.COMMAND,
                db.getName(),
                db.getUser(),
                formatCommandNote(command, cfg.message));
        auditingQueue.offer(doc);
      }
    }
  }

  private String formatCommandNote(final String command, String message) {
    if (message == null || message.isEmpty()) return command;
    return (String)
        OVariableParser.resolveVariables(
            message,
            "${",
            "}",
            new OVariableParserListener() {
              @Override
              public Object resolve(final String iVariable) {
                if (iVariable.startsWith("command")) {
                  return command;
                }
                return null;
              }
            });
  }

  protected void log(final OAuditingOperation operation, final ORecord iRecord) {
    if (auditingQueue == null)
      // LOGGING THREAD INACTIVE, SKIP THE LOG
      return;

    final OAuditingClassConfig cfg = getAuditConfiguration(iRecord);
    if (cfg == null)
      // SKIP
      return;

    ODocument changes = null;
    String note = null;

    switch (operation) {
      case CREATED:
        if (!cfg.onCreateEnabled)
          // SKIP
          return;
        note = cfg.onCreateMessage;
        break;
      case LOADED:
        if (!cfg.onReadEnabled)
          // SKIP
          return;
        note = cfg.onReadMessage;
        break;
      case UPDATED:
        if (!cfg.onUpdateEnabled)
          // SKIP
          return;
        note = cfg.onUpdateMessage;

        if (iRecord instanceof ODocument && cfg.onUpdateChanges) {
          final ODocument doc = (ODocument) iRecord;
          changes = new ODocument();

          for (String f : doc.getDirtyFields()) {
            ODocument fieldChanges = new ODocument();
            fieldChanges.field("from", doc.getOriginalValue(f));
            fieldChanges.field("to", (Object) doc.rawField(f));
            changes.field(f, fieldChanges, OType.EMBEDDED);
          }
        }
        break;
      case DELETED:
        if (!cfg.onDeleteEnabled)
          // SKIP
          return;
        note = cfg.onDeleteMessage;
        break;
    }

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();

    final ODocument doc =
        createLogDocument(operation, db.getName(), db.getUser(), formatNote(iRecord, note));
    doc.field("record", iRecord.getIdentity());
    if (changes != null) doc.field("changes", changes, OType.EMBEDDED);

    if (db.getTransaction().isActive()) {
      synchronized (operations) {
        List<ODocument> oDocuments = operations.get(db);
        if (oDocuments == null) {
          oDocuments = new ArrayList<ODocument>();
          operations.put(db, oDocuments);
        }
        oDocuments.add(doc);
      }
    } else {
      auditingQueue.offer(doc);
    }
  }

  private String formatNote(final ORecord iRecord, final String iNote) {
    if (iNote == null) return null;

    return (String)
        OVariableParser.resolveVariables(
            iNote,
            "${",
            "}",
            new OVariableParserListener() {
              @Override
              public Object resolve(final String iVariable) {
                if (iVariable.startsWith("field.")) {
                  if (iRecord instanceof ODocument) {
                    final String fieldName = iVariable.substring("field.".length());
                    return ((ODocument) iRecord).field(fieldName);
                  }
                }
                return null;
              }
            });
  }

  private OAuditingClassConfig getAuditConfiguration(final ORecord iRecord) {
    OAuditingClassConfig cfg = null;

    if (iRecord instanceof ODocument) {
      OClass cls = ((ODocument) iRecord).getSchemaClass();
      if (cls != null) {

        if (cls.getName().equals(ODefaultAuditing.AUDITING_LOG_CLASSNAME))
          // SKIP LOG CLASS
          return null;

        cfg = classes.get(cls.getName());

        // BROWSE SUPER CLASSES UP TO ROOT
        while (cfg == null && cls != null) {
          cls = cls.getSuperClass();
          if (cls != null) {
            cfg = classes.get(cls.getName());
            if (cfg != null && !cfg.polymorphic) {
              // NOT POLYMORPHIC: IGNORE IT AND EXIT FROM THE LOOP
              cfg = null;
              break;
            }
          }
        }
      }
    }

    if (cfg == null)
      // ASSIGN DEFAULT CFG (*)
      cfg = defaultConfig;

    return cfg;
  }

  public void shutdown(final boolean waitForAllLogs) {
    if (auditingThread != null) {
      auditingThread.sendShutdown(waitForAllLogs);
      auditingQueue = null;
    }
  }

  /*
    private OAuditingClassConfig getAuditConfiguration(OClass cls) {
      OAuditingClassConfig cfg = null;

      if (cls != null) {

        cfg = classes.get(cls.getName());

        // BROWSE SUPER CLASSES UP TO ROOT
        while (cfg == null && cls != null) {
          cls = cls.getSuperClass();

          if (cls != null) {
            cfg = classes.get(cls.getName());

            if (cfg != null && !cfg.polymorphic) {
              // NOT POLYMORPHIC: IGNORE IT AND EXIT FROM THE LOOP
              cfg = null;
              break;
            }
          }
        }
      }

      if (cfg == null)
        // ASSIGN DEFAULT CFG (*)
        cfg = defaultConfig;

      return cfg;
    }
  */
  private String formatClassNote(final OClass cls, final String note) {
    if (note == null || note.isEmpty()) return cls.getName();

    return (String)
        OVariableParser.resolveVariables(
            note,
            "${",
            "}",
            new OVariableParserListener() {
              @Override
              public Object resolve(final String iVariable) {

                if (iVariable.equalsIgnoreCase("class")) {
                  return cls.getName();
                }

                return null;
              }
            });
  }

  protected void logClass(final OAuditingOperation operation, final String note) {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();

    final OSecurityUser user = db.getUser();

    final ODocument doc = createLogDocument(operation, db.getName(), user, note);

    auditingQueue.offer(doc);
  }

  protected void logClass(final OAuditingOperation operation, final OClass cls) {
    if (schemaConfig != null && schemaConfig.isEnabled(operation)) {
      logClass(operation, schemaConfig.formatMessage(operation, cls.getName()));
    }
  }

  public void onCreateClass(OClass iClass) {
    logClass(OAuditingOperation.CREATEDCLASS, iClass);
  }

  public void onDropClass(OClass iClass) {
    logClass(OAuditingOperation.DROPPEDCLASS, iClass);
  }

  public void log(
      final OAuditingOperation operation,
      final String dbName,
      OSecurityUser user,
      final String message) {
    if (auditingQueue != null)
      auditingQueue.offer(createLogDocument(operation, dbName, user, message));
  }

  private ODocument createLogDocument(
      final OAuditingOperation operation,
      final String dbName,
      OSecurityUser user,
      final String message) {
    ODocument doc = null;

    doc = new ODocument();
    doc.field("date", System.currentTimeMillis());
    doc.field("operation", operation.getByte());

    if (user != null) {
      doc.field("user", user.getName());
      doc.field("userType", user.getUserType());
    }

    if (message != null) doc.field("note", message);

    if (dbName != null) doc.field("database", dbName);

    return doc;
  }
}
