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
package com.orientechnologies.security.syslog;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.security.OSyslog;

import com.cloudbees.syslog.*;
import com.cloudbees.syslog.sender.*;

import java.lang.StringBuilder;

/**
 * Provides a default implementation for syslog access.
 * 
 * @author S. Colin Leister
 * 
 */
public class ODefaultSyslog implements OSyslog
{
	private boolean _Debug = false;
	private boolean _Enabled = false;
	private String _Hostname = "localhost";
	private int _Port = 514; // Default syslog UDP port.
	private String _AppName = "OrientDB";

	private UdpSyslogMessageSender _MessageSender;
	
	// OSecurityComponent
	public void active()
	{
		try
		{
			if(isEnabled())
			{
				_MessageSender = new UdpSyslogMessageSender();
//				_MessageSender.setDefaultMessageHostname("myhostname");
//				_MessageSender.setDefaultAppName(_AppName);
//				_MessageSender.setDefaultFacility(Facility.USER);
//				_MessageSender.setDefaultSeverity(Severity.INFORMATIONAL);
				_MessageSender.setSyslogServerHostname(_Hostname);
				_MessageSender.setSyslogServerPort(_Port);
				_MessageSender.setMessageFormat(MessageFormat.RFC_3164); // optional, default is RFC 3164
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultSyslog.active() Exception: %s", ex.getMessage());
		}
	}

	// OSecurityComponent
	public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig)
	{
		try
		{
			if(jsonConfig.containsField("enabled"))
			{
				_Enabled = jsonConfig.field("enabled");
			}

			if(jsonConfig.containsField("debug"))
			{
				_Debug = jsonConfig.field("debug");
			}

			if(jsonConfig.containsField("hostname"))
			{
				_Hostname = jsonConfig.field("hostname");
			}

			if(jsonConfig.containsField("port"))
			{
				_Port = jsonConfig.field("port");
			}

			if(jsonConfig.containsField("appName"))
			{
				_AppName = jsonConfig.field("appName");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultSyslog.config() Exception: %s", ex.getMessage());
		}
	}

	// OSecurityComponent
	public void dispose() { }

	// OSecurityComponent
	public boolean isEnabled()
	{
		return _Enabled;
	}	

	// OSyslog
	public void log(final String operation, final String message)
	{
		log(operation, null, null, message);				
	}
	
	// OSyslog
	public void log(final String operation, final String username, final String message)
	{
		log(operation, null, username, message);		
	}
	
	// OSyslog
	public void log(final String operation, final String dbName, final String username, final String message)
	{
		try
		{
			if(_MessageSender != null)
			{
				SyslogMessage sysMsg = new SyslogMessage();

				sysMsg.setFacility(Facility.USER);
				sysMsg.setSeverity(Severity.INFORMATIONAL);
				
				sysMsg.setAppName(_AppName);
	
				// Sylog ignores these settings.
//				if(operation != null) sysMsg.setMsgId(operation);
//				if(dbName != null) sysMsg.setProcId(dbName);

				StringBuilder sb = new StringBuilder();
				
				if(operation != null)
				{
					sb.append("[");
					sb.append(operation);
					sb.append("] ");
				}
				
				if(dbName != null)
				{
					sb.append("Database: ");
					sb.append(dbName);
					sb.append(" ");
				}
				
				if(username != null)
				{
					sb.append("Username: ");
					sb.append(username);
					sb.append(" ");
				}
				
				if(message != null)
				{
					sb.append(message);
				}
				
				sysMsg.withMsg(sb.toString());
				
				_MessageSender.sendMessage(sysMsg);
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultSyslog.log() Exception: %s", ex.getMessage());
		}
	}
}
