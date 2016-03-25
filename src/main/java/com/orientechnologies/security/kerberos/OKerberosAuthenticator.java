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
package com.orientechnologies.security.kerberos;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.kerberos.OKrb5ClientLoginModuleConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.security.OSecurityAuthenticator;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorAbstract;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorException;

// Temporary, for Java 7 support.
import sun.misc.BASE64Decoder;

import java.lang.StringBuilder;
import java.lang.ThreadLocal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Implements the Kerberos authenticator module.
 * 
 * @author S. Colin Leister
 * 
 */
 
public class OKerberosAuthenticator extends OSecurityAuthenticatorAbstract
{
	private final String KERBEROS_PLUGIN_VERSION = "0.15";

	private String _Client_CCName = System.getenv("KRB5CCNAME");
	private String _Client_KTName = System.getenv("KRB5_CLIENT_KTNAME");
	private String _Client_Principal;
	private boolean _Client_UseTicketCache = false;
	private int _Client_Period = 300; // Default to 5 hours (300 minutes).
	private Timer _RenewalTimer; // Timer used to renew the LDAP client service ticket.

	private String _Krb5_Config = System.getenv("KRB5_CONFIG");

	private String _Service_KTName = System.getenv("KRB5_KTNAME");
	private String _Service_Principal;

	private String _SPNEGO_KTName = System.getenv("KRB5_KTNAME");
	private String _SPNEGO_Principal;

	private Object _AuthenticateSync = new Object();
	private Subject _Client_Subject;	// Used in dbImport() for communicating with LDAP.
	private Subject _Service_Subject; // Used in authenticate() for decrypting service tickets from binary clients.
	private Subject _SPNEGO_Subject; // Used in authenticate() for decrypting service tickets from REST clients.
		
	private Timer _ExpirationTimer;
	private final long TicketRelayExpiration = 600000L; // 10 minutes, do not change
	private final ConcurrentHashMap<String, TicketItem> _TicketRelayMap = new ConcurrentHashMap<String, TicketItem>();


	/***
	 * OSecurityAuthenticator Interface
	 ***/	 
	// Called once the Server is running.
	public void active()
	{
		ExpirationTask task = new ExpirationTask();
		_ExpirationTimer = new Timer(true);
		_ExpirationTimer.scheduleAtFixedRate(task, 30000, TicketRelayExpiration); // Wait 30 seconds before starting

		RenewalTask renewalTask = new RenewalTask();
		_RenewalTimer = new Timer(true);
		_RenewalTimer.scheduleAtFixedRate(renewalTask, _Client_Period * 1000 * 60, _Client_Period * 1000 * 60); // Wait 30 seconds before starting

		OLogManager.instance().info(this, "OrientDB Kerberos Version: " + KERBEROS_PLUGIN_VERSION);

		OLogManager.instance().info(this, "***********************************************");
		OLogManager.instance().info(this, "** OrientDB Kerberos Authenticator Is Active **");
		OLogManager.instance().info(this, "***********************************************");
	}

	// OSecurityAuthenticator
	// Kerberos magic happens here.
	public String authenticate(final String username, final String password)
	{
		// username will contain either the principal or be null.
		// password will contain either a Kerberos 5 service ticket or a SPNEGO ticket.
		String principal = null;

		try
		{
			if(isDebug())
			{
				OLogManager.instance().info(this, "** Authenticating username: %s", username);
			
				if(OKerberosLibrary.isServiceTicket(password))
					OLogManager.instance().info(this, "** Authenticating password: SERVICE TICKET");
				else
				{
					OLogManager.instance().info(this, "** Authenticating password: %s", password);
				}
			}

			if(password != null)
			{
				if(OKerberosLibrary.isServiceTicket(password))
				{
					// We can't call OKerberosLibrary.authenticate() twice with the same service ticket.
					// If we do, the call to context.acceptSecContext() will think it's a replay attack.
					// OServer.openDatabase() will end up calling this method twice if its call to database.open() fails.
					// So, we store the hash of the service ticket and the principal retrieved from the service ticket in an TicketItem,
					// and we use a HashMap to store the ticket for five minutes.
					// If this authenticate() is called more than once, we retrieve the TicketItem for the username, and we compare 
					// the service ticket's hash code.  If they match, we return the principal.
										
					TicketItem ti = getTicket(Integer.toString(password.hashCode()));
					
					if(ti != null && ti.getHashCode() == password.hashCode())
					{
						if(isDebug()) OLogManager.instance().info(this, "OKerberosAuthenticator.authenticate() TicketHash and password Hash are equal, return principal: " + ti.getPrincipal());
						if(isDebug()) OLogManager.instance().info(this, "OKerberosAuthenticator.authenticate() principal: " + ti.getPrincipal());
						
						principal = ti.getPrincipal();
					}
					else
					{
						// Temporary, for Java 7 support.
						byte[] ticket = new BASE64Decoder().decodeBuffer(password);
						
//							byte [] ticket = java.util.Base64.getDecoder().decode(password);
	
//							principal = OKerberosLibrary.authenticate(_Service_Subject, _Service_Principal, username, ticket);

						try
						{
							synchronized(_AuthenticateSync)
							{
								if(OKerberosLibrary.isSPNegoTicket(ticket))
								{
									principal = OKerberosLibrary.getSPNegoSource(_SPNEGO_Subject, _SPNEGO_Principal, ticket);
								}
								else
								{
									principal = OKerberosLibrary.getKerberosSource(_Service_Subject, _Service_Principal, ticket);
								}
							}
						}
						catch(Exception e)
						{
							OLogManager.instance().error(this, "OKerberosAuthenticator.authenticate() Exception: ", e);
						}

						if(isDebug()) OLogManager.instance().info(this, "OKerberosAuthenticator.authenticate() OKerberosLibrary.authenticate() returned " + principal);
						
//							OLogManager.instance().info(this, "OKerberosAuthenticator.authenticate() addTicket hashCode: " + password.hashCode());
							
						// null is an acceptable principal to store so that subsequent calls using the same ticket will immediately return null
						addTicket(Integer.toString(password.hashCode()), password.hashCode(), principal);
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().debug(this, "OKerberosAuthenticator.authenticate() Exception: ", ex);
		}
				
		return principal;
	}

	// OSecurityAuthenticator
	public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument kerbConfig)
	{
		super.config(oServer, serverCfg, kerbConfig);
		
		if(kerbConfig.containsField("krb5_config"))
		{
			_Krb5_Config = OSystemVariableResolver.resolveSystemVariables((String)kerbConfig.field("krb5_config"));
		
			OLogManager.instance().info(this, "Krb5Config = " + _Krb5_Config);
		}

		// service
		if(kerbConfig.containsField("service"))
		{
			ODocument serviceDoc = kerbConfig.field("service");

			if(serviceDoc.containsField("ktname"))
			{
				_Service_KTName = OSystemVariableResolver.resolveSystemVariables((String)serviceDoc.field("ktname"));

				OLogManager.instance().info(this, "Svc ktname = " + _Service_KTName);
			}

			if(serviceDoc.containsField("principal"))
			{
				_Service_Principal = serviceDoc.field("principal");
		
				OLogManager.instance().info(this, "Svc princ = " + _Service_Principal);
			}
		}
		
		// SPNEGO
		if(kerbConfig.containsField("spnego"))
		{
			ODocument spnegoDoc = kerbConfig.field("spnego");

			if(spnegoDoc.containsField("ktname"))
			{
				_SPNEGO_KTName = OSystemVariableResolver.resolveSystemVariables((String)spnegoDoc.field("ktname"));
		
				OLogManager.instance().info(this, "SPNEGO ktname = " + _SPNEGO_KTName);
			}

			if(spnegoDoc.containsField("principal"))
			{
				_SPNEGO_Principal = spnegoDoc.field("principal");

				OLogManager.instance().info(this, "SPNEGO princ = " + _SPNEGO_Principal);
			}
		}

		// client
		if(kerbConfig.containsField("client"))
		{
			ODocument clientDoc = kerbConfig.field("client");

			if(clientDoc.containsField("useTicketCache"))
			{
				_Client_UseTicketCache = (Boolean)clientDoc.field("useTicketCache", OType.BOOLEAN);
		
				OLogManager.instance().info(this, "Client useTicketCache = " + _Client_UseTicketCache);
			}

			if(clientDoc.containsField("principal"))
			{
				_Client_Principal = clientDoc.field("principal");
		
				OLogManager.instance().info(this, "Client princ = " + _Client_Principal);
			}

			if(clientDoc.containsField("ccname"))
			{
				_Client_CCName = OSystemVariableResolver.resolveSystemVariables((String)clientDoc.field("ccname"));
		
				OLogManager.instance().info(this, "Client ccname = " + _Client_CCName);
			}

			if(clientDoc.containsField("ktname"))
			{
				_Client_KTName = OSystemVariableResolver.resolveSystemVariables((String)clientDoc.field("ktname"));
		
				OLogManager.instance().info(this, "Client ktname = " + _Client_KTName);
			}

			if(clientDoc.containsField("renewalPeriod"))
			{
				_Client_Period = clientDoc.field("renewalPeriod");
			}
		}

      // Initialize Kerberos
      initializeKerberos();

		synchronized(_AuthenticateSync)
		{
	      createServiceSubject();
   	   createSpnegoSubject();
		}
      
      createClientSubject();		
	}

	// OSecurityAuthenticator
	// Called on removal of the authenticator.
	public void dispose()
	{
   	if(_ExpirationTimer != null)
   	{
   		_ExpirationTimer.cancel();
   		_ExpirationTimer = null;
   	}

   	if(_RenewalTimer != null)
   	{
   		_RenewalTimer.cancel();
   		_RenewalTimer = null;
   	}
   	
		synchronized(_TicketRelayMap)
		{
			_TicketRelayMap.clear();
		}
	}

	// OSecurityAuthenticator
	public String getAuthenticationHeader(final String databaseName)
	{
		String header = null;

		// SPNEGO support.
//		if(databaseName != null) header = "WWW-Authenticate: Negotiate realm=\"OrientDB db-" + databaseName + "\"";
//		else header = "WWW-Authenticate: Negotiate realm=\"OrientDB Server\"";
	
		header = OHttpUtils.HEADER_AUTHENTICATE_NEGOTIATE; //"WWW-Authenticate: Negotiate";


//		if(databaseName != null) header = "WWW-Authenticate: Negotiate\nWWW-Authenticate: Basic realm=\"OrientDB db-" + databaseName + "\"";
//		else header = "WWW-Authenticate: Negotiate\nWWW-Authenticate: Basic realm=\"OrientDB Server\"";

		return header;
	}

	// OSecurityAuthenticator
	public Subject getClientSubject() { return _Client_Subject; }

	// OSecurityAuthenticator
	public boolean isSingleSignOnSupported() { return true; }


	/***
	 * Kerberos
	 ***/
	private void initializeKerberos()
	{
      if(_Krb5_Config == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator KRB5 Config cannot be null");

		System.setProperty("sun.security.krb5.debug", Boolean.toString(isDebug()));
		System.setProperty("sun.security.spnego.debug", Boolean.toString(isDebug()));

		System.setProperty("java.security.krb5.conf", _Krb5_Config);
		
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
	}

	private void createServiceSubject()
	{
		if(_Service_Principal == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createServiceSubject() Service Principal cannot be null");
		if(_Service_KTName == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createServiceSubject() Service KeyTab cannot be null");
		
		try
		{
			Configuration cfg = new OKrb5LoginModuleConfig(_Service_Principal, _Service_KTName);
			
			OLogManager.instance().info(this, "createServiceSubject() Service Principal: " + _Service_Principal);			

			LoginContext lc = new LoginContext("ignore", null, null, cfg);
			lc.login();
			
			_Service_Subject = lc.getSubject();
			
			if(_Service_Subject != null)
			{
				OKerberosLibrary.checkNativeJGSS(_Service_Subject, _Service_Principal, false);
					
				OLogManager.instance().info(this, "** Created Kerberos Service Subject **");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "createServiceSubject() Exception: ", ex);
		}
		
		if(_Service_Subject == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator could not create service Subject");
	}

	private void createSpnegoSubject()
	{
		if(_SPNEGO_Principal == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createSpnegoSubject() SPNEGO Principal cannot be null");
		if(_SPNEGO_KTName == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createSpnegoSubject() SPNEGO KeyTab cannot be null");
		
		try
		{
			Configuration cfg = new OKrb5LoginModuleConfig(_SPNEGO_Principal, _SPNEGO_KTName);
			
			OLogManager.instance().info(this, "createSpnegoSubject() SPNEGO Principal: " + _SPNEGO_Principal);

			LoginContext lc = new LoginContext("ignore", null, null, cfg);
			lc.login();
			
			_SPNEGO_Subject = lc.getSubject();
			
			if(_SPNEGO_Subject != null)
			{
				OKerberosLibrary.checkNativeJGSS(_SPNEGO_Subject, _SPNEGO_Principal, false);
					
				OLogManager.instance().info(this, "** Created Kerberos SPNEGO Subject **");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "createSpnegoSubject() Exception: ", ex);
		}
		
		if(_SPNEGO_Subject == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator could not create SPNEGO Subject");
	}

	private void createClientSubject()
	{
		if(_Client_Principal == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createClientSubject() Client Principal cannot be null");
		if(_Client_UseTicketCache && _Client_CCName == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createClientSubject() Client UseTicketCache cannot be true while Credential Cache is null");
		if(_Client_CCName == null && _Client_KTName == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator.createClientSubject() Client Credential Cache and Client KeyTab cannot both be null");
		
		try
		{
			Configuration cfg = new OKrb5ClientLoginModuleConfig(_Client_Principal, _Client_UseTicketCache, _Client_CCName, _Client_KTName);

			OLogManager.instance().info(this, "createClientSubject() Client Principal: " + _Client_Principal);

			LoginContext lc = new LoginContext("ignore", null, null, cfg);
			lc.login();
			
			_Client_Subject = lc.getSubject();
			
			if(_Client_Subject != null)
			{
				OKerberosLibrary.checkNativeJGSS(_Client_Subject, _Client_Principal, true);
					
				OLogManager.instance().info(this, "** Created Kerberos Client Subject **");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "createClientSubject() Exception: ", ex);
		}
		
		if(_Client_Subject == null) throw new OSecurityAuthenticatorException("OKerberosAuthenticator could not create client Subject");
	}

	/***
	 * Ticket Cache
	 ***/
	private class TicketItem
	{
		private int _HashCode;
		private String _Principal;
		private long _Time;

		public int getHashCode() { return _HashCode; }
		public String getPrincipal() { return _Principal; }
		
		public TicketItem(int hashCode, String principal)
		{
			_HashCode = hashCode;
			_Principal = principal;
			_Time = System.currentTimeMillis();
		}
		
		public boolean hasExpired(long currTime)
		{
			return (currTime - _Time) >= TicketRelayExpiration;
		}
	}	

	// If the TicketItem already exists for id it is replaced.
	private void addTicket(String id, int hashCode, String principal)
	{
		synchronized(_TicketRelayMap)
		{
			_TicketRelayMap.put(id, new TicketItem(hashCode, principal));
		}
	}

	private TicketItem getTicket(String id)
	{
		TicketItem ti = null;
		
		synchronized(_TicketRelayMap)
		{
			ti = _TicketRelayMap.get(id);
		}
		
		return ti;
	}

	private void removeTicket(String id)
	{
		synchronized(_TicketRelayMap)
		{
			if(_TicketRelayMap.containsKey(id))
			{
				_TicketRelayMap.remove(id);
			}
		}
	}
	
	private void checkTicketExpirations()
	{		
		synchronized(_TicketRelayMap)
		{
			long currTime = System.currentTimeMillis();
			
			for(Map.Entry<String, TicketItem> entry : _TicketRelayMap.entrySet())
			{
				if(entry.getValue().hasExpired(currTime))
				{
//					OLogManager.instance().info(this, "~~~~~~~~ checkTicketExpirations() Ticket has expired: " + entry.getValue().getHashCode() + "\n");
					
					_TicketRelayMap.remove(entry.getKey());
				}
			}
		}
	}

	private class ExpirationTask extends TimerTask
	{
		@Override public void run()
		{
			checkTicketExpirations();
		}
	}

	private class RenewalTask extends TimerTask
	{
		@Override public void run()
		{
			createClientSubject();
		}
	}
}
