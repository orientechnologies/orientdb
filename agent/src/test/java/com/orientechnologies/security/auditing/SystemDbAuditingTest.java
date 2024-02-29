package com.orientechnologies.security.auditing;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.security.AbstractSecurityTest;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author sdipro
 * @since 02/06/16 <<<<<<< HEAD
 *     <p>Launches a new OServer (using the security.json resource). Creates a test database.
 *     Creates a new class called 'TestClass'. Queries the system database auditing log for
 *     'TestClass'. Asserts that the "created class" event is there. Drops the 'TestClass' class.
 *     Queries the system database auditing log for 'TestClass'. Asserts that the "dropped class"
 *     event is there. =======
 *     <p>Launches a new OServer (using the security.json resource). Creates a test database.
 *     Creates a new class called 'TestClass'. Queries the system database auditing log for
 *     'TestClass'. Asserts that the "created class" event is there. Drops the 'TestClass' class.
 *     Queries the system database auditing log for 'TestClass'. Asserts that the "dropped class"
 *     event is there. >>>>>>> 064809e... Add a sleep on auditing test (after class create) to give
 *     auditing mechanisms time to execute
 */
public class SystemDbAuditingTest extends AbstractSecurityTest {

  private static final String TESTDB = "SystemDbAuditingTestDB";

  private static OServer server;
  private static OrientDB remote;

  @BeforeClass
  public static void beforeClass() throws Exception {
    setup(TESTDB);

    createFile(
        SERVER_DIRECTORY + "/config/orientdb-server-config.xml",
        SystemDbAuditingTest.class.getResourceAsStream(
            "/com/orientechnologies/security/auditing/orientdb-server-config.xml"));
    createFile(
        SERVER_DIRECTORY + "/config/security.json",
        SystemDbAuditingTest.class.getResourceAsStream(
            "/com/orientechnologies/security/auditing/security.json"));

    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);

    server.startup(new File(SERVER_DIRECTORY + "/config/orientdb-server-config.xml"));

    server.activate();

    createDirectory(SERVER_DIRECTORY + "/databases/" + TESTDB);
    createFile(
        SERVER_DIRECTORY + "/databases/" + TESTDB + "/auditing-config.json",
        SystemDbAuditingTest.class.getResourceAsStream(
            "/com/orientechnologies/security/auditing/auditing-config.json"));

    remote =
        new OrientDB(
            "remote:localhost",
            "root",
            "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3",
            OrientDBConfig.defaultConfig());
    remote
        .execute(
            "create database "
                + TESTDB
                + " plocal users(admin identified by 'adminpwd' role admin)")
        .close();
  }

  @AfterClass
  public static void afterClass() {
    remote.close();
    server.shutdown();

    cleanup(TESTDB);

    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  @Test
  public void createDropClassTest() throws IOException {
    ODatabaseDocument db = remote.open(TESTDB, "admin", "adminpwd");

    server.getSystemDatabase().execute(null, "delete from OAuditingLog where database = ?", TESTDB);

    db.command("create class TestClass").close();

    try {
      Thread.sleep(
          1000); // let auditing log happen (remove this and make auditing more reliable!!!)
    } catch (InterruptedException e) {
      OLogManager.instance().warn(this, "Thread interrputed", e);
    }
    String query = "select from OAuditingLog where database = ? and note = ?";

    List<OResult> result =
        (List<OResult>)
            server
                .getSystemDatabase()
                .execute(
                    (res) -> res.stream().collect(Collectors.toList()),
                    query,
                    TESTDB,
                    "I created a class: TestClass");

    assertThat(result).isNotNull();

    assertThat(result).hasSize(1);

    // Drop Class Test
    db.command("drop class TestClass").close();

    try {
      Thread.sleep(
          1000); // let auditing log happen (remove this and make auditing more reliable!!!)
    } catch (InterruptedException e) {
      OLogManager.instance().warn(this, "Thread interrupted", e);
    }
    result =
        (List<OResult>)
            server
                .getSystemDatabase()
                .execute(
                    (res) -> res.stream().collect(Collectors.toList()),
                    query,
                    TESTDB,
                    "I dropped a class: TestClass");

    assertThat(result).isNotNull();

    assertThat(result).hasSize(1);

    db.close();
  }
}
