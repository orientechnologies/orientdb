package com.orientechnologies.security.auditing;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.OSystemUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.distributed.http.EEBaseServerHttpTest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class AuditingRootLogTest extends EEBaseServerHttpTest {

  @Test
  public void changePasswordWitRootTest() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    ODatabaseSession session = remote.open(name.getMethodName(), "root", "root");

    session
        .command("update OUser set password = ? where name = ?", new Object[] {"foo", "reader"})
        .close();

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 12")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Thread.sleep(100);
    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        "The password for user 'reader' has been changed", result.getProperty("note"));
  }

  @Test
  public void loginReader() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    ODatabaseSession session = remote.open(name.getMethodName(), "readerServer", "readerServer");

    long count = session.query("select from OUser").stream().count();

    Assert.assertEquals(0, count);
  }

  @Test(expected = OSecurityAccessException.class)
  public void loginGuest() throws Exception {
    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    ODatabaseSession session = remote.open(name.getMethodName(), "guest", "guest");
  }

  @Test
  public void reloadSecurityTest() throws Exception {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    server
        .getSecurity()
        .reload(
            new OSystemUser("root", null, "Server"), new ODocument().fromJSON(security, "noMap"));

    Thread.sleep(1000);

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 11")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        "The security configuration file has been reloaded", result.getProperty("note"));
  }

  @Test
  public void postSecurity() throws IOException, InterruptedException {

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));

    try {
      server
          .getSystemDatabase()
          .executeWithDB(
              (db) -> {
                db.command("delete from OAuditingLog");
                return null;
              });
    } catch (OCommandExecutionException e) {

    }

    ODocument config = new ODocument().fromJSON(security, "noMap");

    ODocument cfg = new ODocument().field("config", config);

    HttpResponse response =
        post("/security/reload").payload(cfg.toJSON(), CONTENT.JSON).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Thread.sleep(1000);

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 11")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        "The security configuration file has been reloaded", result.getProperty("note"));
  }

  @Test
  public void postSecurityAndAuditingConfig() throws IOException {

    server
        .getSystemDatabase()
        .executeWithDB(
            (db) -> {
              db.command("delete from OAuditingLog");
              return null;
            });

    String security =
        OIOUtils.readStreamAsString(
            Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));

    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    String auditing =
        OIOUtils.readStreamAsString(
            Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("auditing-config.json"));

    ODocument config = new ODocument().fromJSON(auditing, "noMap");

    HttpResponse response =
        post("/auditing/" + name.getMethodName() + "/config")
            .payload(config.toJSON(), CONTENT.JSON)
            .getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    List<OResult> results =
        server
            .getSystemDatabase()
            .executeWithDB(
                (db) -> {
                  try (OResultSet resultSet =
                      db.query("select from OAuditingLog where operation = 7")) {
                    return resultSet.stream().collect(Collectors.toList());
                  }
                });

    Assert.assertEquals(1, results.size());

    OResult result = results.get(0);

    Assert.assertEquals("root", result.getProperty("user"));
    Assert.assertEquals("Server", result.getProperty("userType"));
    Assert.assertEquals(
        String.format(
            "The auditing configuration for the database '%s' has been changed",
            name.getMethodName()),
        result.getProperty("note"));
  }
}
