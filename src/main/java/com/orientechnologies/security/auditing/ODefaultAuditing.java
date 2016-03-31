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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
//import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.security.OAuditingService;
import com.orientechnologies.orient.server.security.OSyslog;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 10/04/15.
 */
public class ODefaultAuditing implements OAuditingService, ODatabaseLifecycleListener, ODistributedLifecycleListener {

  private boolean _Enabled = true;
  private OServer _Server;

  private Map<String, OAuditingHook> hooks;

  protected static final String      DEFAULT_FILE_AUDITING_DB_CONFIG = "default-auditing-config.json";
  protected static final String      FILE_AUDITING_DB_CONFIG         = "auditing-config.json";
//  private OServerPluginAbstract serverPlugin;

  public ODefaultAuditing() {
//    this.serverPlugin = serverPlugin;
    hooks = new ConcurrentHashMap<String, OAuditingHook>(20);
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
    final File auditingFileConfig = getConfigFile(iDatabase.getName());
    String content = null;
    if (auditingFileConfig != null && auditingFileConfig.exists()) {
      content = getContent(auditingFileConfig);

    } else {
      final InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE_AUDITING_DB_CONFIG);

if(resourceAsStream == null)      
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
    return new OAuditingHook(cfg, _Server.getSecurity().getSyslog());
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
  public boolean onNodeJoining(String iNode) { return true; }
  
  public void onNodeJoined(String iNode)
  {
    log("node joined", String.format("Node %s joined the cluster", iNode));
  }

  public void onNodeLeft(String iNode)
  {
    log("node left", String.format("Node %s left the cluster", iNode));
  }

  public void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus)
  {

  }
	
	//////
	// OAuditingService
	public void changeConfig(final String iDatabaseName, final ODocument cfg) throws IOException
	{
		hooks.put(iDatabaseName, new OAuditingHook(cfg, _Server.getSecurity().getSyslog()));
		updateConfigOnDisk(iDatabaseName, cfg);
	}

	public ODocument getConfig(final String iDatabaseName)
	{
		return hooks.get(iDatabaseName).getConfiguration();
	}


	public void log(final String operation, final String message)
	{
		log(operation, null, null, message);
	}
	
	public void log(final String operation, final String username, final String message)
	{
		log(operation, null, username, message);
	}
	
	public void log(final String operation, final String dbName, final String username, final String message)
	{
		if(_Server.getSecurity().getSyslog() != null)
		{
			_Server.getSecurity().getSyslog().log(operation, dbName, username, message);
		}		
	}

	//////
	// OAuditingService (OSecurityComponent)

	// Called once the Server is running.
	public void active()
	{
		Orient.instance().addDbLifecycleListener(this);
		
		if(_Server.getDistributedManager() != null)
		{
			_Server.getDistributedManager().registerLifecycleListener(this);
		}
	}
	
	public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig)
	{
		_Server = oServer;
		
		try
		{
			if(jsonConfig.containsField("enabled"))
			{
				_Enabled = jsonConfig.field("enabled");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultAuditing.config() Exception: %s", ex.getMessage());
		}
	}

	// Called on removal of the component.
	public void dispose()
	{
		if(_Server.getDistributedManager() != null)
		{
			_Server.getDistributedManager().unregisterLifecycleListener(this);
		}
		
		Orient.instance().removeDbLifecycleListener(this);
	}	

	// OSecurityComponent
	public boolean isEnabled()
	{
		return _Enabled;
	}
}
