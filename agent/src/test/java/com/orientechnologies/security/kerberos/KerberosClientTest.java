package com.orientechnologies.security.kerberos;

import com.orientechnologies.security.AbstractSecurityTest;

import com.orientechnologies.orient.client.remote.OServerAdmin;
//import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
//import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
//import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
//import java.util.ArrayList;
import java.util.Collection;
//import java.util.List;

//import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author sdipro
 * @since 10/06/16
 * 
 */
public class KerberosClientTest extends AbstractSecurityTest {

  private static final String kerbServer = "kerby.odbrealm.com";
  private static final String testDB = "TestDB";
  private static final String url = "remote:" + kerbServer + "/" + testDB;
  private static final String kerbUser = "orientdb@ODBREALM.COM";
  private static final String spn = "OrientDB/kerby.odbrealm.com";
  private static final String ccache = "/home/jenkins/ccache";	 


  @BeforeClass
  public static void beforeClass() throws Exception {
    OServerAdmin serverAd = new OServerAdmin(kerbServer);
    serverAd.connect("root", "password");
    
    if(!serverAd.existsDatabase(testDB, "plocal")) {
      serverAd.createDatabase(testDB, "graph", "plocal");
      
      // orientdb@ODBREALM.COM
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
      db.open("root", "password");
    
      try {
        // Create the Kerberos client user.
        final String sql = String.format("create user %s identified by %s role %s", kerbUser, "notneeded", "admin");
        db.command(new OCommandSQL(sql)).execute();  
      }
      finally {
       	if (db != null) db.close();
      }
    }

    serverAd.close();

    OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.setValue("com.orientechnologies.orient.core.security.kerberos.OKerberosCredentialInterceptor");
    OGlobalConfiguration.CLIENT_KRB5_CONFIG.setValue("/etc/krb5.conf");
  }

  @AfterClass
  public static void afterClass() {
  }

  @Test
  public void defaultSPNTest() throws InterruptedException, IOException {
	 shellCommand(String.format("echo password | kinit -c %s orientdb", ccache));

    Assert.assertTrue(fileExists(ccache));

    OGlobalConfiguration.CLIENT_KRB5_CCNAME.setValue(ccache);

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(kerbUser, "");

    db.close();
  }

  @Test
  public void explicitSPNTest() throws InterruptedException, IOException {
	 shellCommand(String.format("echo password | kinit -c %s orientdb", ccache));

    Assert.assertTrue(fileExists(ccache));

    OGlobalConfiguration.CLIENT_KRB5_CCNAME.setValue(ccache);

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(kerbUser, spn);

    db.close();
  }

  @Test(expected=OSecurityException.class)
  public void shouldFailAuthenticationTest() throws InterruptedException, IOException {
	 final String wrongcache = "/home/jenkins/wrongcache";	 
	 
	 shellCommand(String.format("echo password | kinit -c %s orientdb", ccache));

    OGlobalConfiguration.CLIENT_KRB5_CCNAME.setValue(wrongcache);

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(kerbUser, spn);

    db.close();
  }
}
