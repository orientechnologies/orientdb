/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.security;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityExternal;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.security.OSecurityFactory;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.OSecuritySystemException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
//import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostSecurityReload;
import com.orientechnologies.orient.server.security.OAuditingService;
import com.orientechnologies.orient.server.security.OPasswordValidator;
import com.orientechnologies.orient.server.security.OSecurityAuthenticator;
import com.orientechnologies.orient.server.security.OSecurityComponent;
import com.orientechnologies.orient.server.security.OSyslog;


/**
 * Provides an implementation of OServerSecurity.
 * 
 * @author S. Colin Leister
 * 
 */

public class ODefaultServerSecurity implements OSecurityFactory, OServerLifecycleListener, OServerSecurity
{
	private boolean _Enabled = false; // Defaults to not enabled at first.
	private boolean _Debug = false;

	private boolean _CreateDefaultUsers = true;
	private boolean _StorePasswords = true;


	// OServerSecurity (via OSecurityAuthenticator)
	// Some external security implementations may permit falling back to a 
	// default authentication mode if external authentication fails.
	private boolean _AllowDefault = true;

	private Object _PasswordValidatorSynch = new Object();
	private OPasswordValidator _PasswordValidator;
	
	private Object _ImportLDAPSynch = new Object();
	private OSecurityComponent _ImportLDAP;

	private Object _AuditingSynch = new Object();
	private OAuditingService _AuditingService;
	
	private Object _SyslogSynch = new Object();
	private OSyslog _Syslog;
	
	private ODocument _ConfigDoc; // Holds the current JSON configuration.
	private OServer _Server;
	private OServerConfigurationManager _ServerConfig;
	
	protected OServer getServer() { return _Server; }

	// The SuperUser is now only used by the ODefaultServerSecurity for self-authentication.
	private final String _SuperUser = "OSecurityModuleSuperUser";
	private String _SuperUserPassword;
	private OServerUserConfiguration _SuperUserCfg;

	// We use a list because the order indicates priority of method.
	private final List<OSecurityAuthenticator> _AuthenticatorsList = new ArrayList<OSecurityAuthenticator>();

	private ConcurrentHashMap<String, Class<?>> _SecurityClassMap = new ConcurrentHashMap<String, Class<?>>();

	
	public ODefaultServerSecurity(final OServer oServer, final OServerConfigurationManager serverCfg)
	{
		_Server = oServer;
		_ServerConfig = serverCfg;
		
      oServer.registerLifecycleListener(this);
	}

	private Class<?> getClass(final ODocument jsonConfig)
	{
		Class<?> cls = null;
		
		try
		{
			if(jsonConfig.containsField("class"))
			{	
				final String clsName = jsonConfig.field("class");
		
				if(_SecurityClassMap.containsKey(clsName))
				{
					cls = _SecurityClassMap.get(clsName);
				}
				else
				{
					cls = Class.forName(clsName);
				}
			}
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.getClass() Throwable: ", th);
		}
		
		return cls;
	}

	// OSecuritySystem (via OServerSecurity)
	// Some external security implementations may permit falling back to a 
	// default authentication mode if external authentication fails.
	public boolean isDefaultAllowed()
	{
		if(isEnabled()) return _AllowDefault;
		else return true; // If the security system is disabled return the original system default.
	}

	// OSecuritySystem (via OServerSecurity)
	public String authenticate(final String username, final String password)
	{
		try
		{
			// It's possible for the username to be null or an empty string in the case of SPNEGO Kerberos tickets.	
			if(username != null && !username.isEmpty())
			{
				if(_Debug)
					OLogManager.instance().info(this, "ODefaultServerSecurity.authenticate() ** Authenticating username: %s", username);
			
				// This means it originates from us (used by openDatabase).
				if(username.equals(_SuperUser) && password.equals(_SuperUserPassword)) return _SuperUser;
			}
			
			synchronized(_AuthenticatorsList)
			{			
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						String principal = sa.authenticate(username, password);
					
						if(principal != null) return principal;
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate() Exception: %s", ex.getMessage());
		}
		
		return null; // Indicates authentication failed.
	}

	// OSecuritySystem (via OServerSecurity)
	// Indicates if OServer should create default users if none exist.
	public boolean areDefaultUsersCreated()
	{ 
		if(isEnabled()) return _CreateDefaultUsers;
		else return true; // If the security system is disabled return the original system default.
	}
	
	// OSecuritySystem (via OServerSecurity)
	// Used for generating the appropriate HTTP authentication mechanism.
	public String getAuthenticationHeader(final String databaseName)
	{
		String header = null;

		// Default to Basic.
		if(databaseName != null) header = "WWW-Authenticate: Basic realm=\"OrientDB db-" + databaseName + "\"";
		else header = "WWW-Authenticate: Basic realm=\"OrientDB Server\"";

		if(isEnabled())
		{
			synchronized(_AuthenticatorsList)
			{
				StringBuilder sb = new StringBuilder();
				
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						String sah = sa.getAuthenticationHeader(databaseName);
						
						if(sah != null)
						{
							sb.append(sah);
							sb.append("\n");
						}
					}
				}
				
				if(sb.length() > 0)
				{
					header = sb.toString();
				}
			}
		}
		
		return header;
	}
	
	// OSecuritySystem (via OServerSecurity)
	public ODocument getConfig() { return _ConfigDoc; }
	
	// OSecuritySystem (via OServerSecurity)
	public ODocument getComponentConfig(final String name) {	return getSection(name); }

	// OSecuritySystem (via OServerSecurity)
	// This will first look for a user in the security.json "users" array and then check if a resource matches.
	public boolean isAuthorized(final String username, final String resource)
	{
		if(isEnabled())
		{
			if(username == null || resource == null) return false;
			
			if(username.equals(_SuperUser)) return true;
			
			synchronized(_AuthenticatorsList)
			{		
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						if(sa.isAuthorized(username, resource)) return true;
					}
				}
			}
		}
		
		return false;
	}

	// OSecuritySystem (via OServerSecurity)
	public boolean isEnabled() { return _Enabled; }

	// OSecuritySystem (via OServerSecurity)
	// Indicates if passwords should be stored for users.
	public boolean arePasswordsStored()
	{ 
		if(isEnabled()) return _StorePasswords;
		else return true; // If the security system is disabled return the original system default.
	}
	
	// OSecuritySystem (via OServerSecurity)
	// Indicates if the primary security mechanism supports single sign-on.
	public boolean isSingleSignOnSupported()
	{ 
		if(isEnabled())
		{
			OSecurityAuthenticator priAuth = getPrimaryAuthenticator();
		
			if(priAuth != null) return priAuth.isSingleSignOnSupported();
		}
		
		return false;
	}

	// OSecuritySystem (via OServerSecurity)
	public void validatePassword(final String password) throws OInvalidPasswordException
	{
		if(isEnabled())
		{
  			synchronized(_PasswordValidatorSynch)
  			{			
				if(_PasswordValidator != null)
				{
					_PasswordValidator.validatePassword(password);
				}
			}
		}
	}

	/***
	 * OServerSecurity Interface
	 ***/

	// OServerSecurity
	public OAuditingService getAuditing() { return _AuditingService; }

	// OServerSecurity
	public OSecurityAuthenticator getAuthenticator(final String authMethod)
	{
		if(isEnabled())
		{
			synchronized(_AuthenticatorsList)
			{
				for(OSecurityAuthenticator am : _AuthenticatorsList)
				{
					// If authMethod is null or an empty string, then return the first OSecurityAuthenticator.
					if(authMethod == null || authMethod.isEmpty()) return am;
					
					if(am.getName() != null && am.getName().equalsIgnoreCase(authMethod)) return am;
				}
			}
		}
		
		return null;
	}

	// OServerSecurity
	// Returns the first OSecurityAuthenticator in the list.
	public OSecurityAuthenticator getPrimaryAuthenticator()
	{
		if(isEnabled())
		{
			synchronized(_AuthenticatorsList)
			{
				if(_AuthenticatorsList.size() > 0) return _AuthenticatorsList.get(0);
			}
		}
		
		return null;
	}

	// OServerSecurity
	public OSyslog getSyslog()
	{
		return _Syslog;
	}
	
	// OServerSecurity
	public OServerUserConfiguration getUser(final String username)
	{
		OServerUserConfiguration userCfg = null;
		
		if(isEnabled())
		{
			if(username.equals(_SuperUser)) return _SuperUserCfg;
		
			synchronized(_AuthenticatorsList)
			{		
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						userCfg = sa.getUser(username);
						if(userCfg != null) break;
					}
				}
			}
		}
		
		return userCfg;
	}

	// OServerSecurity
	public ODatabase<?> openDatabase(final String dbName)
	{
//		final String path = _Server.getStoragePath(dbName);
//		final ODatabaseInternal<?> db = new ODatabaseDocumentTx(path);
		
//		ODatabase<?> db = _Server.openDatabase(dbName, _SuperUser, _SuperUserPassword);
		
		ODatabase<?> db = null;
		
		if(isEnabled())
		{
			db = _Server.openDatabase(dbName, _SuperUser, "", null, true); // true indicates bypassing security.
		}
		
		return db;
	}

	// OSecuritySystem
	public void registerSecurityClass(final Class<?> cls)
	{
		String fullTypeName = getFullTypeName(cls);
		
		if(fullTypeName != null)
		{
			_SecurityClassMap.put(fullTypeName, cls);
		}
	}

	// OSecuritySystem
	public void unregisterSecurityClass(final Class<?> cls)
	{
		String fullTypeName = getFullTypeName(cls);
		
		if(fullTypeName != null)
		{
			_SecurityClassMap.remove(fullTypeName);
		}
	}

	// Returns the package plus type name of Class.
	private static String getFullTypeName(Class<?> type)
	{
		String typeName = null;
		
		typeName = type.getSimpleName();
		
		Package pack = type.getPackage();
		
		if(pack != null)
		{
			typeName = pack.getName() + "." + typeName;
		}		
		
		return typeName;
	}	

	// OSecuritySystem
	public void reload(final String cfgPath)
	{
		reload(loadConfig(cfgPath));
	}

	// OSecuritySystem
	public void reload(final ODocument configDoc)
	{
		if(configDoc != null)
		{
			onBeforeDeactivate();
	
			_ConfigDoc = configDoc;
	
			onAfterActivate();
			
			synchronized(_AuditingSynch)
			{
				if(_AuditingService != null) _AuditingService.log("Reload Security", "The security configuration file has been reloaded");
			}
		}
		else
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.reload(ODocument) The provided configuration document is null");
			throw new OSecuritySystemException("ODefaultServerSecurity.reload(ODocument) The provided configuration document is null");
		}
	}

	public void reloadComponent(final String name, final ODocument jsonConfig)
	{
		if(name == null || name.isEmpty()) throw new OSecuritySystemException("ODefaultServerSecurity.reloadComponent() name is null or empty");
		if(jsonConfig == null) throw new OSecuritySystemException("ODefaultServerSecurity.reloadComponent() Configuration document is null");
		
		if(name.equalsIgnoreCase("auditing"))
		{
			reloadAuditingService(jsonConfig);
		}
		else
		if(name.equalsIgnoreCase("authentication"))
		{
			reloadAuthMethods(jsonConfig);
		}
		else
		if(name.equalsIgnoreCase("ldapImporter"))
		{
			reloadImportLDAP(jsonConfig);
		}
		else
		if(name.equalsIgnoreCase("passwordValidator"))
		{
			reloadPasswordValidator(jsonConfig);
		}
		else
		if(name.equalsIgnoreCase("server"))
		{
			reloadServer(jsonConfig);
		}
		else
		if(name.equalsIgnoreCase("syslog"))
		{
			reloadSyslog(jsonConfig);
		}
	}
	
	private void createSuperUser()
	{
		if(_SuperUser == null) throw new OSecuritySystemException("ODefaultServerSecurity.createSuperUser() SuperUser cannot be null");
		
		try
		{
			// Assign a temporary password so that we know if authentication requests coming from the SuperUser are from us.
			_SuperUserPassword = OSecurityManager.instance().createSHA256(String.valueOf(new java.util.Random().nextLong()));
			
			_SuperUserCfg = new OServerUserConfiguration(_SuperUser, _SuperUserPassword, "*");
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "createSuperUser() Exception: ", ex);
		}
		
		if(_SuperUserPassword == null) throw new OSecuritySystemException("ODefaultServerSecurity Could not create SuperUser");
	}	

	private void loadAuthenticators(final ODocument authDoc)
	{
		synchronized(_AuthenticatorsList)
		{
			for(OSecurityAuthenticator sa : _AuthenticatorsList)
			{
				sa.dispose();
			}

			_AuthenticatorsList.clear();

			if(authDoc.containsField("authenticators"))
			{
				List<ODocument> authMethodsList = authDoc.field("authenticators");

				for(ODocument authMethodDoc : authMethodsList)
				{
					try
					{
						if(authMethodDoc.containsField("name"))
						{
							final String name = authMethodDoc.field("name");

							// defaults to enabled if "enabled" is missing
							boolean enabled = true;
							
							if(authMethodDoc.containsField("enabled"))
								enabled = authMethodDoc.field("enabled");
								
							if(enabled)
							{
								Class<?> authClass = getClass(authMethodDoc);
								
					      	if(authClass != null)
					      	{					      		
					      		if(OSecurityAuthenticator.class.isAssignableFrom(authClass))
					      		{
					      			OSecurityAuthenticator authPlugin = (OSecurityAuthenticator)authClass.newInstance();
					      			
					      			authPlugin.config(_Server, _ServerConfig, authMethodDoc);
					      			authPlugin.active();
					      			
					      			_AuthenticatorsList.add(authPlugin);
					      		}
					      		else
					      		{
					      			OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() class is not an OSecurityAuthenticator");
					      		}
					      	}
						      else
						      {
						      	OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() authentication class is null for %s", name);
						      }								
							}
						}
						else
						{
							OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() authentication object is missing name");
						}
					}
					catch(Throwable ex)
					{
						OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() Exception: ", ex);
					}
				}
			}
		}
		
	}


	/***
	 * OServerLifecycleListener Interface
	 ***/
	public void onBeforeActivate()
	{
		createSuperUser();
      
		// Default
    	String configFile = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");

		// The default "security.json" file can be overridden in the server config file.
		String securityFile = getConfigProperty("server.security.file");
		if(securityFile != null) configFile = securityFile;
		
		String ssf = OGlobalConfiguration.SERVER_SECURITY_FILE.getValueAsString();
		if(ssf != null) configFile = ssf;

		_ConfigDoc = loadConfig(configFile);
	}
	
	// OServerLifecycleListener Interface
   public void onAfterActivate()
   {
   	if(_ConfigDoc != null)
   	{
			loadComponents();
		
			if(isEnabled())
			{
				registerRESTCommands();
						
				OSecurityManager.instance().setSecurityFactory(this);
			}
		}
		else
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.onAfterActivate() Configuration document is empty");
		}
   }

	// OServerLifecycleListener Interface
   public void onBeforeDeactivate()
   {
   	OSecurityManager.instance().setSecurityFactory(null); // Set to default.
   	
   	if(_Enabled)
   	{
			unregisterRESTCommands();
			
			synchronized(_ImportLDAPSynch)
			{
				if(_ImportLDAP != null)
				{
					_ImportLDAP.dispose();
					_ImportLDAP = null;
				}
			}
			
  			synchronized(_PasswordValidatorSynch)
  			{
				if(_PasswordValidator != null)
				{
					_PasswordValidator.dispose();
					_PasswordValidator = null;
				}
			}

			synchronized(_AuditingSynch)
			{
				if(_AuditingService != null)
				{
					_AuditingService.dispose();
					_AuditingService = null;
				}
			}

			synchronized(_SyslogSynch)
			{
				if(_Syslog != null)
				{
					_Syslog.dispose();
					_Syslog = null;
				}
			}

			synchronized(_AuthenticatorsList)
			{
				// Notify all the security components that the server is active.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					sa.dispose();
				}
				
				_AuthenticatorsList.clear();
			}
			
			_Enabled = false;
		}
   }
   
	// OServerLifecycleListener Interface
   public void onAfterDeactivate() {}

	protected void loadComponents()
	{
		// Loads the top-level configuration properties ("enabled" and "debug").
		loadSecurity();
		
		if(isEnabled())
		{
			// Loads the "syslog" configuration properties.
			reloadSyslog(getSection("syslog"));

			// Loads the "auditing" configuration properties.
			reloadAuditingService(getSection("auditing"));

			// Loads the "server" configuration properties.
			reloadServer(getSection("server"));
			
			// Loads the "authentication" configuration properties.
			reloadAuthMethods(getSection("authentication"));
			
			// Loads the "passwordValidator" configuration properties.
			reloadPasswordValidator(getSection("passwordValidator"));
			
			// Loads the "ldapImporter" configuration properties.
			reloadImportLDAP(getSection("ldapImporter"));
		}
	}

	// Returns a section of the JSON document configuration as an ODocument if section is present.
	private ODocument getSection(final String section)
	{
		ODocument sectionDoc = null;
		
		try
		{
			if(_ConfigDoc != null)
			{		
				if(_ConfigDoc.containsField(section))
				{
					sectionDoc = _ConfigDoc.field(section);
				}
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.getSection(%s) Configuration document is null", section);
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.getSection(%s) Exception: %s", section, ex.getMessage());
		}
		
		return sectionDoc;
	}

	// "${ORIENTDB_HOME}/config/security.json"
	private ODocument loadConfig(final String cfgPath)
	{
		ODocument securityDoc = null;

		try
		{
			if(cfgPath != null)
			{			
				// Default
				String jsonFile = OSystemVariableResolver.resolveSystemVariables(cfgPath);
			
				File file = new File(jsonFile);
				
				if(file.exists() && file.canRead())
				{
					FileInputStream fis = null;
					
					try
					{	
						fis = new FileInputStream(file);
									
						final byte[] buffer = new byte[(int)file.length()];
						fis.read(buffer);
					
						securityDoc = (ODocument)new ODocument().fromJSON(new String(buffer), "noMap");
					}
					finally
					{
						if(fis != null) fis.close();
					}
				}
				else
				{
					OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Could not access the security JSON file: %s", jsonFile);
				}
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Configuration file path is null");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Exception: %s", ex.getMessage());
		}
		
		return securityDoc;
	}

	protected String getConfigProperty(final String name)
	{
		String value = null;
	
		if(_Server.getConfiguration() != null && _Server.getConfiguration().properties != null)
		{
			for(OServerEntryConfiguration p : _Server.getConfiguration().properties)
			{
				if(p.name.equals(name))
				{
					value = OSystemVariableResolver.resolveSystemVariables(p.value);
					break;
				}
			}
		}
	
		return value;
	}

	private boolean isEnabled(final ODocument sectionDoc)
	{
		boolean enabled = true;
		
		try
		{
			if(sectionDoc.containsField("enabled"))
			{
				enabled = sectionDoc.field("enabled");
			}			
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.isEnabled() Exception: %s", ex.getMessage());
		}
		
		return enabled;
	}

	
	private void loadSecurity()
	{
		try
		{
			_Enabled = false;
			
			if(_ConfigDoc != null)
			{
				if(_ConfigDoc.containsField("enabled"))
				{
					_Enabled = _ConfigDoc.field("enabled");
				}
		
				if(_ConfigDoc.containsField("debug"))
				{
					_Debug = _ConfigDoc.field("debug");
				}				
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity() jsonConfig is null");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity() Exception: %s", ex.getMessage());
		}
	}

	private void reloadServer(final ODocument serverDoc)
	{
		try
		{
			_CreateDefaultUsers = true;
			_StorePasswords = true;
			
			if(serverDoc != null)
			{
				if(serverDoc.containsField("createDefaultUsers"))
				{
					_CreateDefaultUsers = serverDoc.field("createDefaultUsers");
				}
	
				if(serverDoc.containsField("storePasswords"))
				{
					_StorePasswords = serverDoc.field("storePasswords");
				}				
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadServer() Exception: %s", ex.getMessage());
		}
	}

	private void reloadAuthMethods(final ODocument authDoc)
	{
		if(authDoc != null)
		{
			if(authDoc.containsField("allowDefault"))
			{
				_AllowDefault = authDoc.field("allowDefault");
			}
			
			loadAuthenticators(authDoc);
		}
	}

	private void reloadPasswordValidator(final ODocument pwValidDoc)
	{
		try
		{
  			synchronized(_PasswordValidatorSynch)
  			{
				if(_PasswordValidator != null)
				{
					_PasswordValidator.dispose();
					_PasswordValidator = null;
				}			
				
				if(pwValidDoc != null && isEnabled(pwValidDoc))
				{
					Class<?> cls = getClass(pwValidDoc);
					
					if(cls != null)
					{
		      		if(OPasswordValidator.class.isAssignableFrom(cls))
		      		{
	      				_PasswordValidator = (OPasswordValidator)cls.newInstance();	      				
		      			_PasswordValidator.config(_Server, _ServerConfig, pwValidDoc);
		      			_PasswordValidator.active();
		      		}
		      		else
		      		{
		      			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadPasswordValidator() class is not an OPasswordValidator");
		      		}
					}
					else
					{
						OLogManager.instance().error(this, "ODefaultServerSecurity.reloadPasswordValidator() PasswordValidator class property is missing");
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadPasswordValidator() Exception: %s", ex.getMessage());
		}
	}

	private void reloadImportLDAP(final ODocument importDoc)
	{
		try
		{
			synchronized(_ImportLDAPSynch)
			{
				if(_ImportLDAP != null)
				{
					_ImportLDAP.dispose();
					_ImportLDAP = null;
				}
			
				if(importDoc != null && isEnabled(importDoc))
				{
					Class<?> cls = getClass(importDoc);
					
					if(cls != null)
					{
		      		if(OSecurityComponent.class.isAssignableFrom(cls))
		      		{
	      				_ImportLDAP = (OSecurityComponent)cls.newInstance();
	      				_ImportLDAP.config(_Server, _ServerConfig, importDoc);
	      				_ImportLDAP.active();
		      		}
		      		else
		      		{
		      			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP() class is not an OSecurityComponent");
		      		}
					}
					else
					{
						OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP() ImportLDAP class property is missing");
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP() Exception: %s", ex.getMessage());
		}
	}

	private void reloadAuditingService(final ODocument auditingDoc)
	{
		try
		{
			synchronized(_AuditingSynch)
			{
				if(_AuditingService != null)
				{
					_AuditingService.dispose();
					_AuditingService = null;
				}
				
				if(auditingDoc != null && isEnabled(auditingDoc))
				{
					Class<?> cls = getClass(auditingDoc);
					
					if(cls != null)
					{
		      		if(OAuditingService.class.isAssignableFrom(cls))
		      		{
	      				_AuditingService = (OAuditingService)cls.newInstance();
	      				_AuditingService.config(_Server, _ServerConfig, auditingDoc);
	      				_AuditingService.active();
		      		}
		      		else
		      		{
		      			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService() class is not an OAuditingService");
		      		}
					}
					else
					{
						OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService() Auditing class property is missing");
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService() Exception: %s", ex.getMessage());
		}
	}

	private void reloadSyslog(final ODocument syslogDoc)
	{
		try
		{
			synchronized(_SyslogSynch)
			{
				if(_Syslog != null)
				{
					_Syslog.dispose();
					_Syslog = null;
				}
				
				if(syslogDoc != null && isEnabled(syslogDoc))
				{
					Class<?> cls = getClass(syslogDoc);
					
					if(cls != null)
					{
		      		if(OSyslog.class.isAssignableFrom(cls))
		      		{
	      				_Syslog = (OSyslog)cls.newInstance();
	      				_Syslog.config(_Server, _ServerConfig, syslogDoc);
	      				_Syslog.active();
		      		}
		      		else
		      		{
		      			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadSyslog() class is not an OSyslog");
		      		}
					}
					else
					{
						OLogManager.instance().error(this, "ODefaultServerSecurity.reloadSyslog() Syslog class property is missing");
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.reloadSyslog() Exception: %s", ex.getMessage());
		}
	}

	/***
	 * OSecurityFactory Interface
	 ***/
	public OSecurity newSecurity()
	{
		return new OSecurityExternal();
	}


	private void registerRESTCommands()
	{
		try
		{
			final OServerNetworkListener listener = _Server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

			if(listener != null)
			{
				// Register the REST API Command.
//				listener.registerStatelessCommand(new OServerCommandPostSecurityReload(this));
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.registerRESTCommands() unable to retrieve Network Protocol listener.");
			}
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.registerRESTCommands() Throwable: " + th.getMessage());
		}
	}

	private void unregisterRESTCommands()
	{
		try
		{
			final OServerNetworkListener listener = _Server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

			if(listener != null)
			{
//				listener.unregisterStatelessCommand(OServerCommandPostSecurityReload.class);
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.unregisterRESTCommands() unable to retrieve Network Protocol listener.");
			}
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.unregisterRESTCommands() Throwable: " + th.getMessage());
		}
	}
}
