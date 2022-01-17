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
package com.orientechnologies.security.kerberos;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OImmutableUser;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.security.authenticator.OSecurityAuthenticatorAbstract;
import com.orientechnologies.orient.core.security.kerberos.OKrb5ClientLoginModuleConfig;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorException;
import java.util.Base64;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

// Temporary, for Java 7 support.
// import sun.misc.BASE64Decoder;

/**
 * Implements the Kerberos authenticator module.
 *
 * @author S. Colin Leister
 */
public class OKerberosAuthenticator extends OSecurityAuthenticatorAbstract {
  private final String kerberosPluginVersion = "0.15";
  private final long ticketRelayExpiration = 600000L; // 10 minutes, do not change
  private final ConcurrentHashMap<String, TicketItem> ticketRelayMap =
      new ConcurrentHashMap<String, TicketItem>();
  private String clientCCName = System.getenv("KRB5CCNAME");
  private String clientKTName = System.getenv("KRB5_CLIENT_KTNAME");
  private String clientPrincipal;
  private boolean clientUseTicketCache = false;
  private int clientPeriod = 300; // Default to 5 hours (300 minutes).
  private Timer renewalTimer; // Timer used to renew the LDAP client service ticket.
  private String krb5Config = System.getenv("KRB5_CONFIG");
  private String serviceKTName = System.getenv("KRB5_KTNAME");
  private String servicePrincipal;
  private String spnegoKTName = System.getenv("KRB5_KTNAME");
  private String spnegoPrincipal;
  private Object authenticateSync = new Object();
  private Subject clientSubject; // Used in dbImport() for communicating with LDAP.
  private Subject
      serviceSubject; // Used in authenticate() for decrypting service tickets from binary clients.
  private Subject
      spnegoSubject; // Used in authenticate() for decrypting service tickets from REST clients.
  private Timer expirationTimer;

  /** * OSecurityAuthenticator Interface * */
  // Called once the Server is running.
  public void active() {
    ExpirationTask task = new ExpirationTask();
    expirationTimer = new Timer(true);
    expirationTimer.scheduleAtFixedRate(
        task, 30000, ticketRelayExpiration); // Wait 30 seconds before starting

    RenewalTask renewalTask = new RenewalTask();
    renewalTimer = new Timer(true);
    renewalTimer.scheduleAtFixedRate(
        renewalTask,
        clientPeriod * 1000 * 60,
        clientPeriod * 1000 * 60); // Wait 30 seconds before starting

    OLogManager.instance().info(this, "OrientDB Kerberos Version: " + kerberosPluginVersion);

    OLogManager.instance().info(this, "***********************************************");
    OLogManager.instance().info(this, "** OrientDB Kerberos Authenticator Is Active **");
    OLogManager.instance().info(this, "***********************************************");
  }

  // OSecurityAuthenticator
  // Kerberos magic happens here.
  public OSecurityUser authenticate(
      ODatabaseSession session, final String username, final String password) {
    // username will contain either the principal or be null.
    // password will contain either a Kerberos 5 service ticket or a SPNEGO ticket.
    String principal = null;

    try {
      if (isDebug()) {
        OLogManager.instance().info(this, "** Authenticating username: %s", username);

        if (OKerberosLibrary.isServiceTicket(password))
          OLogManager.instance().info(this, "** Authenticating password: SERVICE TICKET");
        else {
          OLogManager.instance().info(this, "** Authenticating password: %s", password);
        }
      }

      if (password != null) {
        if (OKerberosLibrary.isServiceTicket(password)) {
          // We can't call OKerberosLibrary.authenticate() twice with the same service ticket.
          // If we do, the call to context.acceptSecContext() will think it's a replay attack.
          // OServer.openDatabase() will end up calling this method twice if its call to
          // database.open() fails.
          // So, we store the hash of the service ticket and the principal retrieved from the
          // service ticket in an TicketItem,
          // and we use a HashMap to store the ticket for five minutes.
          // If this authenticate() is called more than once, we retrieve the TicketItem for the
          // username, and we compare
          // the service ticket's hash code.  If they match, we return the principal.

          TicketItem ti = getTicket(Integer.toString(password.hashCode()));

          if (ti != null && ti.getHashCode() == password.hashCode()) {
            if (isDebug())
              OLogManager.instance()
                  .info(
                      this,
                      "OKerberosAuthenticator.authenticate() TicketHash and password Hash are equal, return principal: "
                          + ti.getPrincipal());
            if (isDebug())
              OLogManager.instance()
                  .info(
                      this,
                      "OKerberosAuthenticator.authenticate() principal: " + ti.getPrincipal());

            principal = ti.getPrincipal();
          } else {
            byte[] ticket = Base64.getDecoder().decode(password.getBytes("UTF8"));

            // Temporary, for Java 7 support.
            //						byte[] ticket = new BASE64Decoder().decodeBuffer(password);

            //						byte [] ticket = java.util.Base64.getDecoder().decode(password);

            //						principal = OKerberosLibrary.authenticate(serviceSubject, servicePrincipal,
            // username, ticket);

            try {
              synchronized (authenticateSync) {
                if (OKerberosLibrary.isSPNegoTicket(ticket)) {
                  principal =
                      OKerberosLibrary.getSPNegoSource(spnegoSubject, spnegoPrincipal, ticket);
                } else {
                  principal =
                      OKerberosLibrary.getKerberosSource(serviceSubject, servicePrincipal, ticket);
                }
              }
            } catch (Exception e) {
              OLogManager.instance()
                  .error(this, "OKerberosAuthenticator.authenticate() Exception: ", e);
            }

            if (isDebug())
              OLogManager.instance()
                  .info(
                      this,
                      "OKerberosAuthenticator.authenticate() OKerberosLibrary.authenticate() returned "
                          + principal);

            //							OLogManager.instance().info(this, "OKerberosAuthenticator.authenticate()
            // addTicket hashCode: " + password.hashCode());

            // null is an acceptable principal to store so that subsequent calls using the same
            // ticket will immediately return null
            addTicket(Integer.toString(password.hashCode()), password.hashCode(), principal);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().debug(this, "OKerberosAuthenticator.authenticate() Exception: ", ex);
    }

    return new OImmutableUser(principal, OSecurityUser.SERVER_USER_TYPE);
  }

  // OSecurityAuthenticator
  public void config(final ODocument kerbConfig, OSecuritySystem security) {
    super.config(kerbConfig, security);

    if (kerbConfig.containsField("krb5_config")) {
      krb5Config =
          OSystemVariableResolver.resolveSystemVariables((String) kerbConfig.field("krb5_config"));

      OLogManager.instance().info(this, "Krb5Config = " + krb5Config);
    }

    // service
    if (kerbConfig.containsField("service")) {
      ODocument serviceDoc = kerbConfig.field("service");

      if (serviceDoc.containsField("ktname")) {
        serviceKTName =
            OSystemVariableResolver.resolveSystemVariables((String) serviceDoc.field("ktname"));

        OLogManager.instance().info(this, "Svc ktname = " + serviceKTName);
      }

      if (serviceDoc.containsField("principal")) {
        servicePrincipal = serviceDoc.field("principal");

        OLogManager.instance().info(this, "Svc princ = " + servicePrincipal);
      }
    }

    // SPNEGO
    if (kerbConfig.containsField("spnego")) {
      ODocument spnegoDoc = kerbConfig.field("spnego");

      if (spnegoDoc.containsField("ktname")) {
        spnegoKTName =
            OSystemVariableResolver.resolveSystemVariables((String) spnegoDoc.field("ktname"));

        OLogManager.instance().info(this, "SPNEGO ktname = " + spnegoKTName);
      }

      if (spnegoDoc.containsField("principal")) {
        spnegoPrincipal = spnegoDoc.field("principal");

        OLogManager.instance().info(this, "SPNEGO princ = " + spnegoPrincipal);
      }
    }

    // client
    if (kerbConfig.containsField("client")) {
      ODocument clientDoc = kerbConfig.field("client");

      if (clientDoc.containsField("useTicketCache")) {
        clientUseTicketCache = (Boolean) clientDoc.field("useTicketCache", OType.BOOLEAN);

        OLogManager.instance().info(this, "Client useTicketCache = " + clientUseTicketCache);
      }

      if (clientDoc.containsField("principal")) {
        clientPrincipal = clientDoc.field("principal");

        OLogManager.instance().info(this, "Client princ = " + clientPrincipal);
      }

      if (clientDoc.containsField("ccname")) {
        clientCCName =
            OSystemVariableResolver.resolveSystemVariables((String) clientDoc.field("ccname"));

        OLogManager.instance().info(this, "Client ccname = " + clientCCName);
      }

      if (clientDoc.containsField("ktname")) {
        clientKTName =
            OSystemVariableResolver.resolveSystemVariables((String) clientDoc.field("ktname"));

        OLogManager.instance().info(this, "Client ktname = " + clientKTName);
      }

      if (clientDoc.containsField("renewalPeriod")) {
        clientPeriod = clientDoc.field("renewalPeriod");
      }
    }

    // Initialize Kerberos
    initializeKerberos();

    synchronized (authenticateSync) {
      createServiceSubject();
      createSpnegoSubject();
    }

    createClientSubject();
  }

  // OSecurityAuthenticator
  // Called on removal of the authenticator.
  public void dispose() {
    if (expirationTimer != null) {
      expirationTimer.cancel();
      expirationTimer = null;
    }

    if (renewalTimer != null) {
      renewalTimer.cancel();
      renewalTimer = null;
    }

    synchronized (ticketRelayMap) {
      ticketRelayMap.clear();
    }
  }

  // OSecurityAuthenticator
  public String getAuthenticationHeader(final String databaseName) {
    String header = null;

    // SPNEGO support.
    //		if(databaseName != null) header = "WWW-Authenticate: Negotiate realm=\"OrientDB db-" +
    // databaseName + "\"";
    //		else header = "WWW-Authenticate: Negotiate realm=\"OrientDB Server\"";

    header = OHttpUtils.HEADER_AUTHENTICATE_NEGOTIATE; // "WWW-Authenticate: Negotiate";

    //		if(databaseName != null) header = "WWW-Authenticate: Negotiate\nWWW-Authenticate: Basic
    // realm=\"OrientDB db-" + databaseName + "\"";
    //		else header = "WWW-Authenticate: Negotiate\nWWW-Authenticate: Basic realm=\"OrientDB
    // Server\"";

    return header;
  }

  // OSecurityAuthenticator
  public Subject getClientSubject() {
    return clientSubject;
  }

  // OSecurityAuthenticator
  public boolean isSingleSignOnSupported() {
    return true;
  }

  /** * Kerberos * */
  private void initializeKerberos() {
    if (krb5Config == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator KRB5 Config cannot be null");

    System.setProperty("sun.security.krb5.debug", Boolean.toString(isDebug()));
    System.setProperty("sun.security.spnego.debug", Boolean.toString(isDebug()));

    System.setProperty("java.security.krb5.conf", krb5Config);

    System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
  }

  private void createServiceSubject() {
    if (servicePrincipal == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createServiceSubject() Service Principal cannot be null");
    if (serviceKTName == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createServiceSubject() Service KeyTab cannot be null");

    try {
      Configuration cfg = new OKrb5LoginModuleConfig(servicePrincipal, serviceKTName);

      OLogManager.instance()
          .info(this, "createServiceSubject() Service Principal: " + servicePrincipal);

      LoginContext lc = new LoginContext("ignore", null, null, cfg);
      lc.login();

      serviceSubject = lc.getSubject();

      if (serviceSubject != null) {
        OKerberosLibrary.checkNativeJGSS(serviceSubject, servicePrincipal, false);

        OLogManager.instance().info(this, "** Created Kerberos Service Subject **");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "createServiceSubject() Exception: ", ex);
    }

    if (serviceSubject == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator could not create service Subject");
  }

  private void createSpnegoSubject() {
    if (spnegoPrincipal == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createSpnegoSubject() SPNEGO Principal cannot be null");
    if (spnegoKTName == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createSpnegoSubject() SPNEGO KeyTab cannot be null");

    try {
      Configuration cfg = new OKrb5LoginModuleConfig(spnegoPrincipal, spnegoKTName);

      OLogManager.instance()
          .info(this, "createSpnegoSubject() SPNEGO Principal: " + spnegoPrincipal);

      LoginContext lc = new LoginContext("ignore", null, null, cfg);
      lc.login();

      spnegoSubject = lc.getSubject();

      if (spnegoSubject != null) {
        OKerberosLibrary.checkNativeJGSS(spnegoSubject, spnegoPrincipal, false);

        OLogManager.instance().info(this, "** Created Kerberos SPNEGO Subject **");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "createSpnegoSubject() Exception: ", ex);
    }

    if (spnegoSubject == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator could not create SPNEGO Subject");
  }

  private void createClientSubject() {
    if (clientPrincipal == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createClientSubject() Client Principal cannot be null");
    if (clientUseTicketCache && clientCCName == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createClientSubject() Client UseTicketCache cannot be true while Credential Cache is null");
    if (clientCCName == null && clientKTName == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator.createClientSubject() Client Credential Cache and Client KeyTab cannot both be null");

    try {
      Configuration cfg =
          new OKrb5ClientLoginModuleConfig(
              clientPrincipal, clientUseTicketCache, clientCCName, clientKTName);

      OLogManager.instance()
          .info(this, "createClientSubject() Client Principal: " + clientPrincipal);

      LoginContext lc = new LoginContext("ignore", null, null, cfg);
      lc.login();

      clientSubject = lc.getSubject();

      if (clientSubject != null) {
        OKerberosLibrary.checkNativeJGSS(clientSubject, clientPrincipal, true);

        OLogManager.instance().info(this, "** Created Kerberos Client Subject **");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "createClientSubject() Exception: ", ex);
    }

    if (clientSubject == null)
      throw new OSecurityAuthenticatorException(
          "OKerberosAuthenticator could not create client Subject");
  }

  // If the TicketItem already exists for id it is replaced.
  private void addTicket(String id, int hashCode, String principal) {
    synchronized (ticketRelayMap) {
      ticketRelayMap.put(id, new TicketItem(hashCode, principal));
    }
  }

  private TicketItem getTicket(String id) {
    TicketItem ti = null;

    synchronized (ticketRelayMap) {
      ti = ticketRelayMap.get(id);
    }

    return ti;
  }

  private void removeTicket(String id) {
    synchronized (ticketRelayMap) {
      if (ticketRelayMap.containsKey(id)) {
        ticketRelayMap.remove(id);
      }
    }
  }

  private void checkTicketExpirations() {
    synchronized (ticketRelayMap) {
      long currTime = System.currentTimeMillis();

      for (Map.Entry<String, TicketItem> entry : ticketRelayMap.entrySet()) {
        if (entry.getValue().hasExpired(currTime)) {
          //					OLogManager.instance().info(this, "~~~~~~~~ checkTicketExpirations() Ticket has
          // expired: " + entry.getValue().getHashCode() + "\n");

          ticketRelayMap.remove(entry.getKey());
        }
      }
    }
  }

  /** * Ticket Cache * */
  private class TicketItem {
    private int hashCode;
    private String principal;
    private long time;

    public TicketItem(int hashCode, String principal) {
      this.hashCode = hashCode;
      this.principal = principal;
      time = System.currentTimeMillis();
    }

    public int getHashCode() {
      return hashCode;
    }

    public String getPrincipal() {
      return principal;
    }

    public boolean hasExpired(long currTime) {
      return (currTime - time) >= ticketRelayExpiration;
    }
  }

  private class ExpirationTask extends TimerTask {
    @Override
    public void run() {
      checkTicketExpirations();
    }
  }

  private class RenewalTask extends TimerTask {
    @Override
    public void run() {
      createClientSubject();
    }
  }
}
