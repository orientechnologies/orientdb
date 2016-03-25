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
package com.orientechnologies.security;

import com.orientechnologies.common.log.OLogManager;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import com.orientechnologies.security.auditing.ODefaultAuditing;
import com.orientechnologies.security.authenticator.ODefaultPasswordAuthenticator;
import com.orientechnologies.security.authenticator.OServerConfigAuthenticator;
import com.orientechnologies.security.kerberos.OKerberosAuthenticator;
import com.orientechnologies.security.ldap.OLDAPImporter;
import com.orientechnologies.security.password.ODefaultPasswordValidator;
import com.orientechnologies.security.syslog.ODefaultSyslog;

public class OSecurityPlugin extends OServerPluginAbstract
{
	private OServer _Server;
	
	@Override
	public void config(OServer server, OServerParameterConfiguration[] iParams)
	{
		_Server = server;
	}
	
	@Override
	public String getName() { return "security-plugin"; }
	
	@Override
	public void startup()
	{
      registerSecurityComponents();      
	}
	
	@Override
	public void shutdown()
	{
		unregisterSecurityComponents();
	}

	// The OSecurityModule resides in the main application's class loader.  Its configuration file
	// may reference components that are reside in pluggable modules.
	// A security plugin should register its components so that OSecuritySystem has access to them.
	private void registerSecurityComponents()
	{
		try
		{
			if(_Server.getSecurity() != null)
			{
	      	_Server.getSecurity().registerSecurityClass(ODefaultAuditing.class);
				_Server.getSecurity().registerSecurityClass(ODefaultPasswordAuthenticator.class);
	      	_Server.getSecurity().registerSecurityClass(ODefaultPasswordValidator.class);
	      	_Server.getSecurity().registerSecurityClass(ODefaultSyslog.class);
				_Server.getSecurity().registerSecurityClass(OKerberosAuthenticator.class);
	      	_Server.getSecurity().registerSecurityClass(OLDAPImporter.class);
				_Server.getSecurity().registerSecurityClass(OServerConfigAuthenticator.class);
	      }
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "registerSecurityComponents() Throwable: " + th);
		}
	}	

	private void unregisterSecurityComponents()
	{
		try
		{
			if(_Server.getSecurity() != null)
			{
	      	_Server.getSecurity().unregisterSecurityClass(ODefaultAuditing.class);
				_Server.getSecurity().unregisterSecurityClass(ODefaultPasswordAuthenticator.class);
	      	_Server.getSecurity().unregisterSecurityClass(ODefaultPasswordValidator.class);
	      	_Server.getSecurity().unregisterSecurityClass(ODefaultSyslog.class);
				_Server.getSecurity().unregisterSecurityClass(OKerberosAuthenticator.class);
	      	_Server.getSecurity().unregisterSecurityClass(OLDAPImporter.class);
				_Server.getSecurity().unregisterSecurityClass(OServerConfigAuthenticator.class);
	      }
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "unregisterSecurityComponents() Throwable: " + th);
		}
	}
}
