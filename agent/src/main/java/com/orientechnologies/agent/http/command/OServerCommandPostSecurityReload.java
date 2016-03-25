/*
 * Copyright 2010-2016 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.agent.http.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;

import java.lang.StringBuilder;

import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import com.orientechnologies.orient.server.security.OServerSecurity;

public class OServerCommandPostSecurityReload extends OServerCommandAuthenticatedServerAbstract
{
	private static final String[] NAMES = { "POST|security/reload" };
	
	private OServerSecurity _ServerSecurity;
	
	@Override
	public String[] getNames()	{ return NAMES; }

	public OServerCommandPostSecurityReload(OServerSecurity serverSec)
	{
		super("*");
		
		_ServerSecurity = serverSec;
	}

	@Override
	public boolean beforeExecute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws IOException
	{
		return authenticate(iRequest, iResponse, false);
	}
	
	@Override
	public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception
	{
		if(iRequest.content == null)
		{
	   	WriteError(iResponse, "OServerCommandPostSecurityReload.execute()", "Request Content is null");
	   	return false;
		}

      if(_ServerSecurity == null)
      {
			WriteError(iResponse, "OServerCommandPostSecurityReload.execute()", "ServerSecurity is null");
      	return false;
      }

		try
		{
	      // Convert the JSON content to an ODocument to make parsing it easier.
	      final ODocument jsonParams = new ODocument().fromJSON(iRequest.content, "noMap");
	      
	      // These are required fields.
	      if(!jsonParams.containsField("configFile"))
	      {
				WriteError(iResponse, "OServerCommandPostSecurityReload.execute()", "/security/reload config is missing");
	      	return false;
	      }
	
			final String configName = OSystemVariableResolver.resolveSystemVariables((String)jsonParams.field("configFile"));
			
			OLogManager.instance().info(this, "OServerCommandPostSecurityReload.execute() configName = %s", configName);
			
			_ServerSecurity.reload(configName);
		}
		catch(Exception ex)
		{
			WriteError(iResponse, "OServerCommandPostSecurityReload.execute()", "Exception: " + ex.getMessage());
			return false;
		}
		
		WriteJSON(iResponse, "Configuration loaded successfully");
		
		return false;
	}
	
	protected void WriteError(final OHttpResponse iResponse, final String method, final String reason)
	{
		try
		{
			OLogManager.instance().error(this, "%s %s", method, reason);
			
			final StringBuilder json = new StringBuilder();
			
			json.append("{ \"Status\" : \"Error\", \"Reason\" : \"");
			json.append(reason);
			json.append("\" }");
        
			iResponse.send(OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Error", OHttpUtils.CONTENT_JSON, json.toString(), null);
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "OServerCommandPostSecurityReload.WriteJSON() Exception: " + ex);
		}
	}

	protected void WriteJSON(final OHttpResponse iResponse, final String json)
	{
		try
		{
			iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, json, null);
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "OServerCommandPostSecurityReload.WriteJSON() Exception: " + ex);
		}
	}
}
