package com.orientechnologies.security.auditing;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.distributed.http.EEBaseServerHttpTest;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class AuditingChangePasswordTest extends EEBaseServerHttpTest {

  @Test
  public void changePasswordTest() throws Exception {

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

    ODatabaseSession session = remote.open(name.getMethodName(), "admin", "admin");

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

    Assert.assertEquals("admin", result.getProperty("user"));
    Assert.assertEquals("Database", result.getProperty("userType"));
    Assert.assertEquals(
        "The password for user 'reader' has been changed", result.getProperty("note"));
  }
}
