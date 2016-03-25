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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.security.ldap;

import java.net.URI;
import java.net.URISyntaxException;

public class OLDAPServer
{
	private String _Scheme, _Host;
	private int _Port;
	private boolean _IsAlias;
	
	public String getHostname() { return _Host; }

	public String getURL()
	{
		return String.format("%s://%s:%d", _Scheme, _Host, _Port);
	}

	// Replaces the current URL's host port with hostname and returns it.
	public String getURL(final String hostname)
	{
		return String.format("%s://%s:%d", _Scheme, hostname, _Port);
	}

	public boolean isAlias() { return _IsAlias; }

	public OLDAPServer(final String scheme, final String host, int port, boolean isAlias)
	{
		_Scheme = scheme;
		_Host = host;
		_Port = port;
		_IsAlias = isAlias;
	}
	
	public static OLDAPServer validateURL(final String url, boolean isAlias)
	{
		OLDAPServer server = null;
		
		try
		{
			URI uri = new URI(url);
			
			String scheme 	= uri.getScheme();
			String host 	= uri.getHost();
			int port 		= uri.getPort();
			if(port == -1) port = 389; // Default to standard LDAP port.
			
			server = new OLDAPServer(scheme, host, port, isAlias);
		}
		catch(URISyntaxException se)
		{
			
		}
		
		return server;
	}
	
}
