package com.orientechnologies.agent;

import com.orientechnologies.agent.hook.OAuditingHook;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 10/04/15.
 */
public class OAuditingListener implements ODatabaseLifecycleListener {

  private Map<String, OAuditingHook> hooks;

  protected static final String      DEFAULT_FILE_AUDITING_DB_CONFIG = "default-auditing-config.json";
  protected static final String      FILE_AUDITING_DB_CONFIG         = "auditing-config.json";
  private OEnterpriseAgent           enterpriseAgent;

  public OAuditingListener(final OEnterpriseAgent iEnterpriseAgent) {
    this.enterpriseAgent = iEnterpriseAgent;
    hooks = new ConcurrentHashMap<String, OAuditingHook>(20);
    Orient.instance().addDbLifecycleListener(this);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    final OAuditingHook hook = defaultHook(iDatabase);
    hooks.put(iDatabase.getName(), hook);
    iDatabase.registerHook(hook);
    iDatabase.registerListener(hook);
  }

  private OAuditingHook defaultHook(final ODatabaseInternal iDatabase) {
    final File auditingFileConfig = getAuditingFileConfig(iDatabase.getName());
    String content = null;
    if (auditingFileConfig.exists()) {
      content = getContent(auditingFileConfig);

    } else {
      final InputStream resourceAsStream = OEnterpriseAgent.class.getClassLoader().getResourceAsStream(
              DEFAULT_FILE_AUDITING_DB_CONFIG);
      content = getString(resourceAsStream);
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
    final ODocument cfg = new ODocument().fromJSON(content, "noMap");
    return new OAuditingHook(cfg);
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
  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {
  }

  public File getAuditingFileConfig(final String iDatabaseName) {
    return new File(enterpriseAgent.server.getDatabaseDirectory() + iDatabaseName + "/" + FILE_AUDITING_DB_CONFIG);
  }

  public void changeConfig(final String iDatabaseName, final ODocument cfg) throws IOException {
    hooks.put(iDatabaseName, new OAuditingHook(cfg));
    updateConfigOnDisk(iDatabaseName, cfg);
  }

  protected void updateConfigOnDisk(final String iDatabaseName, final ODocument cfg) throws IOException {
    final File auditingFileConfig = getAuditingFileConfig(iDatabaseName);
    final FileOutputStream f = new FileOutputStream(auditingFileConfig);
    f.write(cfg.toJSON("prettyPrint=true").getBytes());
    f.flush();
  }

  public ODocument getConfig(final String iDatabaseName) {
    return hooks.get(iDatabaseName).getConfiguration();
  }
}
