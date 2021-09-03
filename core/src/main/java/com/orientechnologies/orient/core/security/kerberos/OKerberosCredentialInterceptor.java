/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.security.kerberos;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Base64;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * Provides a Kerberos credential interceptor.
 *
 * @author S. Colin Leister
 */
public class OKerberosCredentialInterceptor implements OCredentialInterceptor {
  private String principal;
  private String serviceTicket;

  public String getUsername() {
    return this.principal;
  }

  public String getPassword() {
    return this.serviceTicket;
  }

  public void intercept(final String url, final String principal, final String spn)
      throws OSecurityException {
    // While the principal can be determined from the ticket cache, if a client keytab is used
    // instead,
    // it may contain multiple principals.
    if (principal == null || principal.isEmpty())
      throw new OSecurityException("OKerberosCredentialInterceptor Principal cannot be null!");

    this.principal = principal;

    String actualSPN = spn;

    // spn should be the SPN of the service.
    if (spn == null || spn.isEmpty()) {
      // If spn is null or an empty string, the SPN will be generated from the URL like this:
      //		OrientDB/host
      if (url == null || url.isEmpty())
        throw new OSecurityException(
            "OKerberosCredentialInterceptor URL and SPN cannot both be null!");

      try {
        String tempURL = url;

        // Without the // URI can't parse URLs correctly, so we add //.
        if (tempURL.startsWith("remote:") && !tempURL.startsWith("remote://"))
          tempURL = tempURL.replace("remote:", "remote://");

        URI remoteURI = new URI(tempURL);

        String host = remoteURI.getHost();

        if (host == null)
          throw new OSecurityException(
              "OKerberosCredentialInterceptor Could not create SPN from URL: " + url);

        actualSPN = "OrientDB/" + host;
      } catch (URISyntaxException ex) {
        throw OException.wrapException(
            new OSecurityException(
                "OKerberosCredentialInterceptor Could not create SPN from URL: " + url),
            ex);
      }
    }

    // Defaults to the environment variable.
    String config = System.getenv("KRB5_CONFIG");
    String ckc = OGlobalConfiguration.CLIENT_KRB5_CONFIG.getValueAsString();
    if (ckc != null) config = ckc;

    // Defaults to the environment variable.
    String ccname = System.getenv("KRB5CCNAME");
    String ccn = OGlobalConfiguration.CLIENT_KRB5_CCNAME.getValueAsString();
    if (ccn != null) ccname = ccn;

    // Defaults to the environment variable.
    String ktname = System.getenv("KRB5_CLIENT_KTNAME");
    String ckn = OGlobalConfiguration.CLIENT_KRB5_KTNAME.getValueAsString();
    if (ckn != null) ktname = ckn;

    if (config == null)
      throw new OSecurityException("OKerberosCredentialInterceptor KRB5 Config cannot be null!");
    if (ccname == null && ktname == null)
      throw new OSecurityException(
          "OKerberosCredentialInterceptor KRB5 Credential Cache and KeyTab cannot both be null!");

    LoginContext lc = null;

    try {
      System.setProperty("java.security.krb5.conf", config);

      OKrb5ClientLoginModuleConfig cfg =
          new OKrb5ClientLoginModuleConfig(principal, ccname, ktname);

      lc = new LoginContext("ignore", null, null, cfg);
      lc.login();
    } catch (LoginException lie) {
      OLogManager.instance().debug(this, "intercept() LoginException", lie);

      throw OException.wrapException(
          new OSecurityException("OKerberosCredentialInterceptor Client Validation Exception!"),
          lie);
    }

    Subject subject = lc.getSubject();

    // Assign the client's principal name.
    //		this.principal = getFirstPrincipal(subject);

    //		if(this.principal == null) throw new OSecurityException("OKerberosCredentialInterceptor
    // Cannot obtain client principal!");

    this.serviceTicket = getServiceTicket(subject, principal, actualSPN);

    try {
      lc.logout();
    } catch (LoginException loe) {
      OLogManager.instance().debug(this, "intercept() LogoutException", loe);
    }

    if (this.serviceTicket == null)
      throw new OSecurityException(
          "OKerberosCredentialInterceptor Cannot obtain the service ticket!");
  }

  private String getFirstPrincipal(Subject subject) {
    if (subject != null) {
      final Object[] principals = subject.getPrincipals().toArray();
      final Principal p = (Principal) principals[0];

      return p.getName();
    }

    return null;
  }

  private String getServiceTicket(
      final Subject subject, final String principal, final String servicePrincipalName) {
    try {
      GSSManager manager = GSSManager.getInstance();
      GSSName serviceName = manager.createName(servicePrincipalName, GSSName.NT_USER_NAME);

      Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");

      // Initiator.
      final GSSContext context =
          manager.createContext(serviceName, krb5Oid, null, GSSContext.DEFAULT_LIFETIME);

      if (context != null) {
        // http://docs.oracle.com/javase/6/docs/technotes/guides/security/jgss/jgss-features.html
        // When performing operations as a particular Subject, e.g. Subject.doAs(...) or
        // Subject.doAsPrivileged(...),
        // the to-be-used GSSCredential should be added to Subject's private credential set.
        // Otherwise,
        // the GSS operations will fail since no credential is found.
        boolean useNativeJgss = Boolean.getBoolean("sun.security.jgss.native");

        if (useNativeJgss) {
          OLogManager.instance().info(this, "getServiceTicket() Using Native JGSS");

          try {
            GSSName clientName = manager.createName(principal, GSSName.NT_USER_NAME);

            // null: indicates using the default principal.
            GSSCredential cred =
                manager.createCredential(
                    clientName, GSSContext.DEFAULT_LIFETIME, krb5Oid, GSSCredential.INITIATE_ONLY);

            subject.getPrivateCredentials().add(cred);
          } catch (GSSException gssEx) {
            OLogManager.instance()
                .error(this, "getServiceTicket() Use Native JGSS GSSException", gssEx);
          }
        }

        // The GSS context initiation has to be performed as a privileged action.
        byte[] serviceTicket =
            Subject.doAs(
                subject,
                new PrivilegedAction<byte[]>() {
                  public byte[] run() {
                    try {
                      byte[] token = new byte[0];

                      // This is a one pass context initialisation.
                      context.requestMutualAuth(false);
                      context.requestCredDeleg(false);
                      return context.initSecContext(token, 0, token.length);
                    } catch (Exception inner) {
                      OLogManager.instance()
                          .debug(this, "getServiceTicket() doAs() Exception", inner);
                    }

                    return null;
                  }
                });

        if (serviceTicket != null) return Base64.getEncoder().encodeToString(serviceTicket);

        context.dispose();
      } else {
        OLogManager.instance().debug(this, "getServiceTicket() GSSContext is null!");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "getServiceTicket() Exception", ex);
    }

    return null;
  }
}
