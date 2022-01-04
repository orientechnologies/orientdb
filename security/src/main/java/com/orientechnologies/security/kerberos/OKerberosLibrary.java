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
import java.security.PrivilegedAction;
import javax.security.auth.Subject;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * Kerberos Authentication Library
 *
 * @author S. Colin Leister
 */
public class OKerberosLibrary {
  /*
  	// Returns the principal if successful, otherwise null.
  	public static String authenticate(Subject subject, final String servicePrincipal, final String principal, final byte [] ticket)
  	{
  		String client = null;

  		try
  		{
  			if(isSPNegoTicket(ticket))
  			{
  				client = getSPNegoSource(subject, servicePrincipal, ticket);

  				OLogManager.instance().info(null, "OKerberosLibrary.authenticate() getSPNegoSource() principal: %s, client: %s", principal, client);
  			}
  			else
  			{
  				client = getKerberosSource(subject, servicePrincipal, ticket);

  				OLogManager.instance().info(null, "OKerberosLibrary.authenticate() getKerberosSource() principal: %s, client: %s", principal, client);
  			}

  //			if(client != null && principal != null && client.equals(principal.toLowerCase())) return true;
  		}
  		catch(Exception ex)
  		{
  			OLogManager.instance().info(null, "OKerberosLibrary.authenticate() Exception: ", ex);
  		}

  		return client;
  	}
  */

  // If native JGSS is specified, adds the credentials to the Subject's private credentials.
  public static void checkNativeJGSS(
      final Subject subject, final String principal, final boolean initiate) {
    /*
    					try
    					{
    						GSSManager managerX = GSSManager.getInstance();

    //						Oid spnegoOidX = new Oid("1.3.6.1.5.5.2");

    						GSSName serviceName = managerX.createName("OrientDB@AD.SDICLOUD.COM", GSSName.NT_USER_NAME);

    						GSSCredential serverCredsX = managerX.createCredential(null, GSSCredential.DEFAULT_LIFETIME, (Oid)null, GSSCredential.INITIATE_ONLY);

    						OLogManager.instance().info(null, "OKerberosLibrary.getSPNegoSource() Kerberos credentialX name = "
    						+ serverCredsX.getName().toString());
    					}
    					catch(Exception exX)
    					{
    						OLogManager.instance().error(null, "OKerberosLibrary.getSPNegoSource() ExceptionX: ", exX);
    					}


    */
    try {
      // http://docs.oracle.com/javase/6/docs/technotes/guides/security/jgss/jgss-features.html
      // When performing operations as a particular Subject, e.g. Subject.doAs(...) or
      // Subject.doAsPrivileged(...),
      // the to-be-used GSSCredential should be added to Subject's private credential set.
      // Otherwise, the GSS operations will fail since no credential is found.
      boolean useNativeJgss = Boolean.getBoolean("sun.security.jgss.native");

      if (useNativeJgss) {
        OLogManager.instance()
            .info(
                null,
                "OKerberosLibrary.checkNativeJGSS() Using Native JGSS, Principal = " + principal);

        int usage = GSSCredential.INITIATE_ONLY;

        if (!initiate) usage = GSSCredential.ACCEPT_ONLY;

        GSSManager manager = GSSManager.getInstance();

        GSSName serviceName = manager.createName(principal, GSSName.NT_USER_NAME);

        // Standard Kerberos
        Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");

        OLogManager.instance()
            .info(
                null,
                "OKerberosLibrary.checkNativeJGSS() calling createCredential() for Kerberos OID");

        // null: indicates using the default principal.
        GSSCredential kerbCreds =
            manager.createCredential(serviceName, GSSCredential.DEFAULT_LIFETIME, krb5Oid, usage);
        subject.getPrivateCredentials().add(kerbCreds);

        OLogManager.instance()
            .info(
                null,
                "OKerberosLibrary.checkNativeJGSS() Kerberos credential name = "
                    + kerbCreds.getName().toString());

        // SPNEGO
        Oid spnegoOid = new Oid("1.3.6.1.5.5.2");

        OLogManager.instance()
            .info(
                null,
                "OKerberosLibrary.checkNativeJGSS() calling createCredential() for SPNEGO OID");

        // null: indicates using the default principal.
        GSSCredential spnegoCreds =
            manager.createCredential(serviceName, GSSCredential.DEFAULT_LIFETIME, spnegoOid, usage);
        subject.getPrivateCredentials().add(spnegoCreds);

        OLogManager.instance()
            .info(
                null,
                "OKerberosLibrary.checkNativeJGSS() Kerberos credential name = "
                    + spnegoCreds.getName().toString());
      }
    } catch (Exception ex) {
      OLogManager.instance().error(null, "OKerberosLibrary.checkNativeJGSS() Exception: ", ex);
    }
  }

  // Returns null if not found or unsuccessful.
  public static String getSPNegoSource(
      final Subject subject, final String principal, final byte[] ticket) {
    // Accept the context and return the client principal name.
    return Subject.doAs(
        subject,
        new PrivilegedAction<String>() {
          public String run() {
            String source = null;

            try {
              GSSManager manager = GSSManager.getInstance();

              GSSName serviceName = manager.createName(principal, GSSName.NT_USER_NAME);

              Oid spnegoOid = new Oid("1.3.6.1.5.5.2");

              // null: indicates using the default principal.

              //						GSSName serviceName = manager.createName("HTTP/ad.sdicloud.com",
              // GSSName.NT_USER_NAME);
              //						GSSName serviceName = manager.createName("OrientDBClient@AD.SDICLOUD.COM",
              // GSSName.NT_HOSTBASED_SERVICE);

              GSSCredential serverCreds =
                  manager.createCredential(
                      serviceName,
                      GSSCredential.DEFAULT_LIFETIME,
                      spnegoOid,
                      GSSCredential.ACCEPT_ONLY);

              OLogManager.instance()
                  .info(
                      null,
                      "OKerberosLibrary.getSPNegoSource() Kerberos credential name = "
                          + serverCreds.getName().toString());

              // Default acceptor.
              GSSContext context = manager.createContext((GSSCredential) serverCreds);

              if (context != null) {
                if (!context.isEstablished()) {
                  context.acceptSecContext(ticket, 0, ticket.length);
                }

                if (context.getSrcName() != null) {
                  OLogManager.instance()
                      .info(
                          this,
                          "OKerberosLibrary.getSPNegoSource() context srcName = "
                              + context.getSrcName());

                  source = context.getSrcName().toString();
                }

                context.dispose();
              } else {
                OLogManager.instance()
                    .error(
                        this,
                        "OKerberosLibrary.getSPNegoSource() Could not create a GSSContext",
                        null);
              }
            } catch (Exception ex) {
              OLogManager.instance()
                  .error(this, "OKerberosLibrary.getSPNegoSource() Exception: ", ex);
            }

            return source;
          }
        });
  }

  // Returns null if not found or unsuccessful.
  public static String getKerberosSource(
      final Subject subject, final String principal, final byte[] ticket) {
    // Accept the context and return the client principal name.
    return Subject.doAs(
        subject,
        new PrivilegedAction<String>() {
          public String run() {
            String source = null;

            try {
              GSSManager manager = GSSManager.getInstance();

              GSSName serviceName = manager.createName(principal, GSSName.NT_USER_NAME);

              Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");

              // null: indicates using the default principal.
              GSSCredential serverCreds =
                  manager.createCredential(
                      serviceName,
                      GSSCredential.DEFAULT_LIFETIME,
                      krb5Oid,
                      GSSCredential.ACCEPT_ONLY);

              OLogManager.instance()
                  .info(
                      null,
                      "OKerberosLibrary.getKerberosSource() Kerberos credential name = "
                          + serverCreds.getName().toString());

              // Default acceptor.
              GSSContext context = manager.createContext((GSSCredential) serverCreds);

              if (context != null) {
                if (!context.isEstablished()) {
                  context.acceptSecContext(ticket, 0, ticket.length);

                  if (context.getSrcName() != null) {
                    source = context.getSrcName().toString();
                  }
                }

                context.dispose();
              } else {
                OLogManager.instance()
                    .error(this, "getKerberosSource() Could not create a GSSContext", null);
              }
            } catch (Exception ex) {
              OLogManager.instance()
                  .error(null, "OKerberosLibrary.getKerberosSource() Exception: ", ex);
            }

            return source;
          }
        });
  }

  // Detects if the ticket is SPNEGO or pure Kerberos.
  private static final byte[] SPENGO_OID = {0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02};

  public static boolean isSPNegoTicket(final byte[] ticket) {
    if (ticket == null || ticket.length < 2) return false;

    return isNegTokenInit(ticket) || isNegTokenArg(ticket);
  }

  private static boolean isNegTokenInit(final byte[] ticket) {
    // Application Constructed Object
    if (ticket[0] != 0x60) return false;

    // Token Length
    int len = 1;
    if ((ticket[1] & 0x80) != 0) {
      len = 1 + (ticket[1] & 0x7f);
    }

    if (ticket.length < SPENGO_OID.length + 1 + len) return false;

    // Look for SPNEGO OID
    for (int i = 0; i < SPENGO_OID.length; i++) {
      if (SPENGO_OID[i] != ticket[i + 1 + len]) return false;
    }

    return true;
  }

  private static boolean isNegTokenArg(final byte[] ticket) {
    // NegTokenArg: 0xa1
    if ((ticket[0] & 0xff) != 0xa1) return false;

    int lenBytes;
    int len;

    if ((ticket[1] & 0x80) == 0) {
      len = ticket[1];
    } else {
      lenBytes = ticket[1] & 0x7f;
      len = 0;
      final int i = 2;

      while (lenBytes > 0) {
        len = len << 8;
        len |= (ticket[i] & 0xff);
        --lenBytes;
      }
    }

    return ticket.length == len + 2;
  }

  public static boolean isServiceTicket(final String ticket) {
    return (ticket != null && ticket.startsWith("YI"));
  }
}
