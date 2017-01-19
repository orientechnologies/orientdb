package com.orientechnologies.security.password;

import com.orientechnologies.security.AbstractSecurityTest;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author sdipro
 * @since 10/06/16
 * 
 * Creates SERVER_DIRECTORY and a 'config' directory.
 * Loads customized 'orientdb-server-config.xml' and 'security.json' files under config.
 * Launches a new OServer (using the security.json resource).
 * Uses OServerAdmin to create a new 'TestDB' database.
 * Tests using a UUID.
 * Tests the minimum count of total characters (5).
 * Tests the minimum count of number characters (2).
 * Tests the minimum count of special characters (2).
 * Tests the minimum count of uppercase characters (3).
 * Tests using a valid UUID as a password.
 * Tests a valid password meeting all criteria.
 */
public class PasswordValidatorTest extends AbstractSecurityTest {

  private static final String TESTDB = "PasswordValidatorTestDB";
  private static final String DATABASE_URL = "remote:localhost/" + TESTDB;

  private static OServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
  	 setup(TESTDB);
  	
  	 createFile(SERVER_DIRECTORY + "/config/orientdb-server-config.xml", PasswordValidatorTest.class.getResourceAsStream("/com/orientechnologies/security/password/orientdb-server-config.xml"));
  	 createFile(SERVER_DIRECTORY + "/config/security.json", PasswordValidatorTest.class.getResourceAsStream("/com/orientechnologies/security/password/security.json"));
  	
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

  @Test
  public void minCharacterTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    db.open("root", ROOT_PASSWORD);
    
    try {
      final String sql = String.format("create user %s identified by %s role %s", "testuser", "pass", "admin");
      db.command(new OCommandSQL(sql)).execute();
    } catch (Exception ex) {
    	
      assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
    }
    
    db.close();
  }

  @Test
  public void minNumberTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    db.open("root", ROOT_PASSWORD);

    try {
      final String sql = String.format("create user %s identified by %s role %s", "testuser", "passw", "admin");
      db.command(new OCommandSQL(sql)).execute();
    } catch (Exception ex) {
      assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
    }

    db.close();
  }
    
  @Test
  public void minSpecialTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    db.open("root", ROOT_PASSWORD);

    try {
      final String sql = String.format("create user %s identified by %s role %s", "testuser", "passw12", "admin");
      db.command(new OCommandSQL(sql)).execute();
    } catch (Exception ex) {
      assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
    }
    
    db.close();
  }

  @Test
  public void minUppercaseTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    db.open("root", ROOT_PASSWORD);

    try {

      final String sql = String.format("create user %s identified by %s role %s", "testuser", "passw12$$", "admin");
      db.command(new OCommandSQL(sql)).execute();
    } catch (Exception ex) {
      assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
    }

    db.close();
  }

  @Test
  public void uuidTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    db.open("root", ROOT_PASSWORD);

    final String sql = String.format("create user %s identified by %s role %s", "uuiduser", java.util.UUID.randomUUID().toString(), "admin");
    db.command(new OCommandSQL(sql)).execute();

    db.close();
  }
  
  @Test
  public void validTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(DATABASE_URL);
    db.open("root", ROOT_PASSWORD);

    final String sql = String.format("create user %s identified by %s role %s", "testuser", "PASsw12$$", "admin");
    db.command(new OCommandSQL(sql)).execute();

    db.close();
  }
}
