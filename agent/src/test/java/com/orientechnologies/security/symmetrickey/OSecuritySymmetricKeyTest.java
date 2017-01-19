package com.orientechnologies.security.symmetrickey;

import com.orientechnologies.orient.client.remote.OServerAdmin;
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
public class OSecuritySymmetricKeyTest extends AbstractSecurityTest {

  private static final String TESTDB = "SecuritySymmetricKeyTestDB";
  private static final String DATABASE_URL = "remote:localhost/" + TESTDB;

  private static OServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
  	 setup(TESTDB);
  	
  	 createFile(SERVER_DIRECTORY + "/config/orientdb-server-config.xml", OSecuritySymmetricKeyTest.class.getResourceAsStream("/com/orientechnologies/security/symmetrickey/orientdb-server-config.xml"));
  	 createFile(SERVER_DIRECTORY + "/config/security.json", OSecuritySymmetricKeyTest.class.getResourceAsStream("/com/orientechnologies/security/symmetrickey/security.json"));
  	 
  	 // Create a default AES 128-bit key.
  	 OSymmetricKey sk = new OSymmetricKey("AES", "AES/CBC/PKCS5Padding", 128);
  	 sk.saveToKeystore(new FileOutputStream(SERVER_DIRECTORY + "/config/test.jks"), "password", "keyAlias", "password");
  	 sk.saveToStream(new FileOutputStream(SERVER_DIRECTORY + "/config/AES.key"));
  	   	 
//  	 createFile(SERVER_DIRECTORY + "/config/test.jks", OSymmetricKeyTest.class.getResourceAsStream("/com/orientechnologies/security/symmetrickey/test.jks"));
  	
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));
    server.activate();
    
    OServerAdmin serverAd = new OServerAdmin("remote:localhost");
    serverAd.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    serverAd.createDatabase(TESTDB, "graph", "plocal");
    serverAd.close();
  }

  @AfterClass
  public static void afterClass() {
    server.shutdown();

    cleanup(TESTDB);
  }


  @Test(expected=OSecurityAccessException.class)
  public void shouldTestSpecificAESKeyFailure() throws Exception {
  	 OSymmetricKey sk = new OSymmetricKey("AES", "AAC7LeGkFbmHEYNTz5GwDw==");

    // "test" is the username.  It's specified in the security.json resource file.
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    db.open("test", sk.encrypt("AES/CBC/PKCS5Padding", "test"));
    db.close();
  }

  @Test
  public void shouldTestSpecificAESKey() throws Exception {
    // This key is specified in the security.json resource file.
  	 OSymmetricKey sk = new OSymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");

    // "test" is the username.  It's specified in the security.json resource file.
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    db.open("test", sk.encrypt("AES/CBC/PKCS5Padding", "test"));
    db.close();
  }

  @Test
  public void shouldTestKeyFile() throws Exception {
    OSymmetricKey sk = OSymmetricKey.fromStream("AES", new FileInputStream(SERVER_DIRECTORY + "/config/AES.key"));
    
    // "test2" is the username.  It's specified in the security.json resource file.
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    db.open("test2", sk.encrypt("AES/CBC/PKCS5Padding", "test2"));
    db.close();
  }

  @Test
  public void shouldTestKeystore() throws Exception {
    OSymmetricKey sk = OSymmetricKey.fromKeystore(new FileInputStream(SERVER_DIRECTORY + "/config/test.jks"), "password", "keyAlias", "password");
    
    // "test3" is the username.  It's specified in the security.json resource file.
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    db.open("test3", sk.encrypt("AES/CBC/PKCS5Padding", "test3"));
    db.close();
  }
}
