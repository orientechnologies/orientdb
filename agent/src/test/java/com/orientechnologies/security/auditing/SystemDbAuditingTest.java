package com.orientechnologies.security.auditing;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.security.AbstractSecurityTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

//import com.orientechnologies.orient.core.exception.OConfigurationException;

/**
 * @author sdipro
 * @since 02/06/16
 * 
 * Launches a new OServer (using the security.json resource).
 * Creates a test database.
 * Creates a new class called 'TestClass'.
 * Queries the system database auditing log for 'TestClass'. 
 * Asserts that the "created class" event is there. 
 * Drops the 'TestClass' class.
 * Queries the system database auditing log for 'TestClass'.
 * Asserts that the "dropped class" event is there. 
 */
public class SystemDbAuditingTest extends AbstractSecurityTest {

  private static final String TESTDB = "SystemDbAuditingTestDB";
  private static final String DATABASE_URL = "remote:localhost/" + TESTDB;

  private static OServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
  	 setup(TESTDB);

  	 createFile(SERVER_DIRECTORY + "/config/orientdb-server-config.xml", SystemDbAuditingTest.class.getResourceAsStream("/com/orientechnologies/security/auditing/orientdb-server-config.xml"));
  	 createFile(SERVER_DIRECTORY + "/config/security.json", SystemDbAuditingTest.class.getResourceAsStream("/com/orientechnologies/security/auditing/security.json"));

    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);   

    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));

    server.activate();

  	 createDirectory(SERVER_DIRECTORY + "/databases/" + TESTDB);
    createFile(SERVER_DIRECTORY + "/databases/" + TESTDB + "/auditing-config.json", SystemDbAuditingTest.class.getResourceAsStream("/com/orientechnologies/security/auditing/auditing-config.json"));
  	
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
  public void createDropClassTest() throws IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/" + TESTDB);
    db.open("admin","admin");

    server.getSystemDatabase().execute(null, "delete from OAuditingLog where database = ?", TESTDB);
    
    db.command(new OCommandSQL("create class TestClass")).execute();
    
    String query = "select from OAuditingLog where database = ? and note = ?";
    
    List<ODocument> result = (List<ODocument>)server.getSystemDatabase().execute(null, query, TESTDB, "I created a class: TestClass");
    
    assertThat(result).isNotNull();

    assertThat(result).hasSize(1);

    // Drop Class Test
    db.command(new OCommandSQL("drop class TestClass")).execute();
    
    result = (List<ODocument>)server.getSystemDatabase().execute(null, query, TESTDB, "I dropped a class: TestClass");

    assertThat(result).isNotNull();

    assertThat(result).hasSize(1);

    db.close();
  }
}
