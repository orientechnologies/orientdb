package com.orientechnologies.security.symmetrickey;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKey;
import com.orientechnologies.orient.server.OServer;

import com.orientechnologies.security.AbstractSecurityTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author S. Colin Leister
 * 
 */
public class OSymmetricKeyCITest extends AbstractSecurityTest {

  private static final String TESTDB = "SymmetricKeyCITestDB";
  private static final String DATABASE_URL = "remote:localhost/" + TESTDB;

  private static OServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
  	 setup(TESTDB);
  	
  	 createFile(SERVER_DIRECTORY + "/config/orientdb-server-config.xml", OSymmetricKeyCITest.class.getResourceAsStream("/com/orientechnologies/security/symmetrickey/orientdb-server-config.xml"));
  	 createFile(SERVER_DIRECTORY + "/config/security.json", OSymmetricKeyCITest.class.getResourceAsStream("/com/orientechnologies/security/symmetrickey/security.json"));
  	 
  	 // Create a default AES 128-bit key.
  	 OSymmetricKey sk = new OSymmetricKey("AES", "AES/CBC/PKCS5Padding", 128);
  	 sk.saveToKeystore(new FileOutputStream(SERVER_DIRECTORY + "/config/test.jks"), "password", "keyAlias", "password");
  	 sk.saveToStream(new FileOutputStream(SERVER_DIRECTORY + "/config/AES.key"));
  	   	 
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));
    server.activate();    
  }

  @AfterClass
  public static void afterClass() {
    server.shutdown();

    cleanup(TESTDB);
  }


  @Test
  public void shouldTestSymmetricKeyCIKey() throws Exception {
  	 OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.setValue("com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKeyCI");
  	 
  	 OGlobalConfiguration.CLIENT_CI_KEYALGORITHM.setValue("AES");
  	 OGlobalConfiguration.CLIENT_CI_CIPHERTRANSFORM.setValue("AES/CBC/PKCS5Padding");
  	 
    // This key is specified in the security.json resource file for username "test".
  	 final String password = "{'key':'8BC7LeGkFbmHEYNTz5GwDw=='}";
  	 
    OServerAdmin serverAd = new OServerAdmin("remote:localhost");
    serverAd.connect("test", password);
    serverAd.close();
  }

  @Test(expected=OSecurityAccessException.class)
  public void shouldTestSymmetricKeyCIKeyFailure() throws Exception {
  	 OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.setValue("com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKeyCI");
  	 
  	 OGlobalConfiguration.CLIENT_CI_KEYALGORITHM.setValue("AES");
  	 OGlobalConfiguration.CLIENT_CI_CIPHERTRANSFORM.setValue("AES/CBC/PKCS5Padding");
  	 
  	 // Set the key to an invalid one.
  	 final String password = "{'key':'AAC7LeGkFbmHEYNTz5GwDw=='}";
  	 
    OServerAdmin serverAd = new OServerAdmin("remote:localhost");
    // The key is specified for username "test" in the security.json file.
    serverAd.connect("test", password);
    serverAd.close();
  }

  @Test
  public void shouldTestSymmetricKeyCIKeyFile() throws Exception {
  	 OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.setValue("com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKeyCI");
  	 
  	 OGlobalConfiguration.CLIENT_CI_KEYALGORITHM.setValue("AES");
  	 OGlobalConfiguration.CLIENT_CI_CIPHERTRANSFORM.setValue("AES/CBC/PKCS5Padding");
  	 
  	 final String password = "{'keyFile':'" + SERVER_DIRECTORY + "/config/AES.key'}";
  	 
  	 // The key file is specified for username "test2" in the security.json file.
    OServerAdmin serverAd = new OServerAdmin("remote:localhost");
    serverAd.connect("test2", password);
    serverAd.close();
  }

  @Test
  public void shouldTestSymmetricKeyCIKeyStore() throws Exception {
  	 OGlobalConfiguration.CLIENT_CREDENTIAL_INTERCEPTOR.setValue("com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKeyCI");
  	 
  	 OGlobalConfiguration.CLIENT_CI_KEYALGORITHM.setValue("AES");
  	 OGlobalConfiguration.CLIENT_CI_CIPHERTRANSFORM.setValue("AES/CBC/PKCS5Padding");
  	 
  	 final String password = "{'keyStore':{ 'file':'" + SERVER_DIRECTORY + "/config/test.jks', 'password':'password', 'keyAlias':'keyAlias', 'keyPassword':'password' } }";
  	 
  	 // The keystore is specified for username "test3" in the security.json file.
    OServerAdmin serverAd = new OServerAdmin("remote:localhost");
    serverAd.connect("test3", password);
    serverAd.close();
  }
}
