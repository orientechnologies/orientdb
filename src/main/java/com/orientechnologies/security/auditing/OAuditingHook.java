/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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

import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.security.OAuditingLog;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hook to audit database access.
 *
 * @author Luca Garulli
 */
public class OAuditingHook extends ORecordHookAbstract implements ODatabaseListener {
  public static final String                      AUDITING_LOG_DEF_CLASSNAME = "AuditingLog";
  private final String                            auditClassName;
  private final Map<String, OAuditingClassConfig> classes                    = new HashMap<String, OAuditingClassConfig>(20);
  private final OAuditingLoggingThread            auditingThread;

  private Map<ODatabaseDocument, List<ODocument>> operations                 = new ConcurrentHashMap<ODatabaseDocument, List<ODocument>>();
  private volatile LinkedBlockingQueue<ODocument> auditingQueue;
  private Set<OAuditingCommandConfig>             commands                   = new HashSet<OAuditingCommandConfig>();
  private boolean                                 onGlobalCreate;
  private boolean                                 onGlobalRead;
  private boolean                                 onGlobalUpdate;
  private boolean                                 onGlobalDelete;
  private boolean                                 onGlobalCreateClass;
  private boolean                                 onGlobalDropClass;

  public static final byte                        COMMAND                    = 4;
  public static final byte                        CREATECLASS                = 5;
  public static final byte                        DROPCLASS                  = 6;
  private OAuditingClassConfig                    defaultConfig              = new OAuditingClassConfig();
  private ODocument    iConfiguration;
  private OAuditingLog syslog;

  private static class OAuditingCommandConfig {
    public String regex;
    public String message;

    public OAuditingCommandConfig(final ODocument cfg) {
      regex = cfg.field("regex");
      message = cfg.field("message");
    }
  }

  private static class OAuditingClassConfig {
    public boolean polymorphic     = true;
    public boolean onCreateEnabled = false;
    public String  onCreateMessage;
    public boolean onReadEnabled   = false;
    public String  onReadMessage;
    public boolean onUpdateEnabled = false;
    public String  onUpdateMessage;
    public boolean onUpdateChanges = true;
    public boolean onDeleteEnabled = false;
    public String  onDeleteMessage;
    public boolean onCreateClassEnabled = false;
    public String  onCreateClassMessage;
    public boolean onDropClassEnabled = false;
    public String  onDropClassMessage;

    public OAuditingClassConfig() {
    }

    public OAuditingClassConfig(final ODocument cfg) {
      if (cfg.containsField("polymorphic"))
        polymorphic = cfg.field("polymorphic");

      // CREATE
      if (cfg.containsField("onCreateEnabled"))
        onCreateEnabled = cfg.field("onCreateEnabled");
      if (cfg.containsField("onCreateMessage"))
        onCreateMessage = cfg.field("onCreateMessage");

      // READ
      if (cfg.containsField("onReadEnabled"))
        onReadEnabled = cfg.field("onReadEnabled");
      if (cfg.containsField("onReadMessage"))
        onReadMessage = cfg.field("onReadMessage");

      // UPDATE
      if (cfg.containsField("onUpdateEnabled"))
        onUpdateEnabled = cfg.field("onUpdateEnabled");
      if (cfg.containsField("onUpdateMessage"))
        onUpdateMessage = cfg.field("onUpdateMessage");
      if (cfg.containsField("onUpdateChanges"))
        onUpdateChanges = cfg.field("onUpdateChanges");

      // DELETE
      if (cfg.containsField("onDeleteEnabled"))
        onDeleteEnabled = cfg.field("onDeleteEnabled");
      if (cfg.containsField("onDeleteMessage"))
        onDeleteMessage = cfg.field("onDeleteMessage");

      // CREATECLASS
      if (cfg.containsField("onCreateClassEnabled"))
        onCreateClassEnabled = cfg.field("onCreateClassEnabled");
      if (cfg.containsField("onCreateClassMessage"))
        onCreateClassMessage = cfg.field("onCreateClassMessage");

      // DROPCLASS
      if (cfg.containsField("onDropClassEnabled"))
        onDropClassEnabled = cfg.field("onDropClassEnabled");
      if (cfg.containsField("onDropClassMessage"))
        onDropClassMessage = cfg.field("onDropClassMessage");
    }
  }

  public OAuditingHook(final String iConfiguration) {
    this(new ODocument().fromJSON(iConfiguration, "noMap"), null);
  }

  public OAuditingHook(final String iConfiguration, final OAuditingLog syslog) {
    this(new ODocument().fromJSON(iConfiguration, "noMap"), syslog);
  }

  public OAuditingHook(final ODocument iConfiguration) {
  	 this(iConfiguration, null);
  }
  
  public OAuditingHook(final ODocument iConfiguration, final OAuditingLog syslog) {
    this.iConfiguration = iConfiguration;
    if (iConfiguration.containsField("auditClassName"))
      auditClassName = iConfiguration.field("auditClassName");
    else
      auditClassName = AUDITING_LOG_DEF_CLASSNAME;

    this.syslog = syslog;

    onGlobalCreate = onGlobalRead = onGlobalUpdate = onGlobalDelete = false;
    onGlobalCreateClass = onGlobalDropClass = false;

    final ODocument classesCfg = iConfiguration.field("classes");
    if (classesCfg != null) {
      for (String c : classesCfg.fieldNames()) {
        final OAuditingClassConfig cfg = new OAuditingClassConfig((ODocument) classesCfg.field(c));
        if (c.equals("*"))
          defaultConfig = cfg;
        else
          classes.put(c, cfg);

        if (cfg.onCreateEnabled)
          onGlobalCreate = true;
        if (cfg.onReadEnabled)
          onGlobalRead = true;
        if (cfg.onUpdateEnabled)
          onGlobalUpdate = true;
        if (cfg.onDeleteEnabled)
          onGlobalDelete = true;

        if (cfg.onCreateClassEnabled)
          onGlobalCreateClass = true;
        if (cfg.onDropClassEnabled)
          onGlobalDropClass = true;
      }
    }

    if (onGlobalCreate || onGlobalRead || onGlobalUpdate || onGlobalDelete || onGlobalCreateClass || onGlobalDropClass) {
      // ENABLE IT
      createClassIfNotExists();
    }
    final Iterable<ODocument> commandCfg = iConfiguration.field("commands");

    if (commandCfg != null) {

      for (ODocument cfg : commandCfg) {
        commands.add(new OAuditingCommandConfig(cfg));
      }
      if (commands.size() > 0) {
        createClassIfNotExists();
      }
    }

    auditingQueue = new LinkedBlockingQueue<ODocument>();
    auditingThread = new OAuditingLoggingThread(ODatabaseRecordThreadLocal.INSTANCE.get().getURL(),
        ODatabaseRecordThreadLocal.INSTANCE.get().getName(), auditingQueue);

    auditingThread.start();
  }

  @Override
  public void onCreate(ODatabase iDatabase) {
  }

  @Override
  public void onDelete(ODatabase iDatabase) {
  }

  @Override
  public void onOpen(ODatabase iDatabase) {
  }

  @Override
  public void onBeforeTxBegin(ODatabase iDatabase) {
  }

  @Override
  public void onBeforeTxRollback(ODatabase iDatabase) {
  }

  @Override
  public void onAfterTxRollback(ODatabase iDatabase) {

    synchronized (operations) {
      operations.remove(iDatabase);
    }
  }

  @Override
  public void onBeforeTxCommit(ODatabase iDatabase) {

  }

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
  public void onClose(ODatabase iDatabase) {
  }

  @Override
  public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {
  }

  @Override
  public void onAfterCommand(OCommandRequestText iCommand, OCommandExecutor executor, Object result) {
    logCommand(iCommand.getText());
  }

  @Override
  public boolean onCorruptionRepairDatabase(ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
    return false;
  }

  private void createClassIfNotExists() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

    OSchema schema = db.getMetadata().getSchema();
    OClass cls = schema.getClass(auditClassName);
    if (cls == null) {
      // CREATE THE CLASS WITH ALL PROPERTIES (SCHEMA-FULL)
      try {
        cls = db.getMetadata().getSchema().createClass(auditClassName);
        cls.createProperty("date", OType.DATETIME);
        cls.createProperty("user", OType.LINK);
        cls.createProperty("operation", OType.BYTE);
        cls.createProperty("record", OType.LINK);
        cls.createProperty("changes", OType.EMBEDDED);
        cls.createProperty("note", OType.STRING);
      } catch (OSchemaException e) {
        // Schema Exists
      }
    }
  }

  public ODocument getConfiguration() {
    return iConfiguration;
  }

  @Override
  public void onRecordAfterCreate(final ORecord iRecord) {
    if (!onGlobalCreate)
      return;

    log(ORecordOperation.CREATED, iRecord);
  }

  @Override
  public void onRecordAfterRead(final ORecord iRecord) {
    if (!onGlobalRead)
      return;

    log(ORecordOperation.LOADED, iRecord);
  }

  @Override
  public void onRecordAfterUpdate(final ORecord iRecord) {
    if (!onGlobalUpdate)
      return;

    log(ORecordOperation.UPDATED, iRecord);
  }

  @Override
  public void onRecordAfterDelete(final ORecord iRecord) {
    if (!onGlobalDelete)
      return;

    log(ORecordOperation.DELETED, iRecord);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
  }

  protected void logCommand(final String command) {

    for (OAuditingCommandConfig cfg : commands) {
      if (command.matches(cfg.regex)) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
        final ODocument doc = new ODocument(auditClassName);
        doc.field("date", System.currentTimeMillis());
        final OSecurityUser user = db.getUser();
        if (user != null)
          doc.field("user", user.getIdentity());
        doc.field("operation", COMMAND);
        doc.field("note", formatCommandNote(command, cfg.message));
        doc.save();
      }
    }
  }

  private String formatCommandNote(final String command, String message) {
    if (message == null || message.isEmpty())
      return command;
    return (String) OVariableParser.resolveVariables(message, "${", "}", new OVariableParserListener() {
      @Override
      public Object resolve(final String iVariable) {
        if (iVariable.startsWith("command")) {
          return command;
        }
        return null;
      }
    });
  }

  protected void log(final byte iOperation, final ORecord iRecord) {
    if (auditingQueue == null)
      // LOGGING THREAD UNACTIVE, SKIP THE LOG
      return;

    final OAuditingClassConfig cfg = getAuditConfiguration(iRecord);
    if (cfg == null)
      // SKIP
      return;

    ODocument changes = null;
    String note = null;

    switch (iOperation) {
    case ORecordOperation.CREATED:
      if (!cfg.onCreateEnabled)
        // SKIP
        return;
      note = cfg.onCreateMessage;
      break;
    case ORecordOperation.LOADED:
      if (!cfg.onReadEnabled)
        // SKIP
        return;
      note = cfg.onReadMessage;
      break;
    case ORecordOperation.UPDATED:
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
          fieldChanges.field("to", doc.rawField(f));
          changes.field(f, fieldChanges, OType.EMBEDDED);
        }
      }
      break;
    case ORecordOperation.DELETED:
      if (!cfg.onDeleteEnabled)
        // SKIP
        return;
      note = cfg.onDeleteMessage;
      break;

    }

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final ODocument doc = new ODocument(auditClassName);
    doc.field("date", System.currentTimeMillis());
    final OSecurityUser user = db.getUser();
    if (user != null)
      doc.field("user", user.getIdentity());
    doc.field("operation", iOperation);
    doc.field("record", iRecord.getIdentity());
    if (changes != null)
      doc.field("changes", changes, OType.EMBEDDED);
    if (note != null)
      doc.field("note", formatNote(iRecord, note));

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
    if (iNote == null)
      return null;

    return (String) OVariableParser.resolveVariables(iNote, "${", "}", new OVariableParserListener() {
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
        if (cls.getName().equals(auditClassName))
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


  private String getOperationAsString(byte operation)
  {
  	 String str = null;
  	 
    switch(operation)
    {
    case ORecordOperation.CREATED:
      str = "created";
      break;
    case ORecordOperation.LOADED:
      str = "loaded";
      break;
    case ORecordOperation.UPDATED:
      str = "updated";
      break;
    case ORecordOperation.DELETED:
      str = "deleted";
      break;
    case COMMAND:
      str = "command";
      break;
    case CREATECLASS:
      str = "createClass";
      break;
    case DROPCLASS:
      str = "dropClass";
      break;
    }
    
    return str;
  }

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

  private String formatClassNote(final OClass cls, final String note) {
    if (note == null || note.isEmpty())
      return cls.getName();

    return (String) OVariableParser.resolveVariables(note, "${", "}", new OVariableParserListener() {
      @Override
      public Object resolve(final String iVariable) {
        if (iVariable.equalsIgnoreCase("class")) {
        	 return cls.getName();
        }

        return null;
      }
    });
  }

  protected void logClass(final byte operation, final String note) {

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final ODocument doc = new ODocument(auditClassName);
    doc.field("date", System.currentTimeMillis());
    final OSecurityUser user = db.getUser();
    if(user != null) doc.field("user", user.getIdentity());
        
    doc.field("operation", operation);
    doc.field("note", note);
    
    auditingQueue.offer(doc);

    if(syslog != null) syslog.log(getOperationAsString(operation), db.getName(), user != null ? user.getName() : null, note);
  }

  protected void logClass(final byte operation, final OClass cls) {

    final OAuditingClassConfig cfg = getAuditConfiguration(cls);
    
    if (cfg == null)
      // SKIP
      return;

    String note = null;

    if(operation == CREATECLASS)
    {
      if (!cfg.onCreateClassEnabled)
        // SKIP
        return;

      note = cfg.onCreateClassMessage;
    }
    else
    if(operation == DROPCLASS)
    {
      if (!cfg.onDropClassEnabled)
        // SKIP
        return;

      note = cfg.onDropClassMessage;    	
    }

    logClass(operation, formatClassNote(cls, note));
  }

  public void onCreateClass(OClass iClass) {
    if (!onGlobalCreateClass)
      return;

  	 logClass(CREATECLASS, iClass);
  }

  public void onDropClass(OClass iClass) {
    // Always report dropping of the audit class.
    if (iClass.getName().equals(auditClassName))
      logClass(DROPCLASS, String.format("Dropped Auditing Class %s", iClass.getName()));

    if (!onGlobalDropClass)
      return;

  	 logClass(DROPCLASS, iClass);
  }
}
