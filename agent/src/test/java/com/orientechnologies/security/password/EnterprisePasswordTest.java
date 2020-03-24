package com.orientechnologies.security.password;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.distributed.http.EEBaseServerHttpTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class EnterprisePasswordTest extends EEBaseServerHttpTest {
//
//  @Test
  public void passwordValidationTest() throws Exception {

    String security = OIOUtils
        .readStreamAsString(Thread.currentThread().getContextClassLoader().getResourceAsStream("security.json"));
    server.getSecurity().reload(new ODocument().fromJSON(security, "noMap"));

    try {
      ODatabaseSession session = remote.open(name.getMethodName(), "admin", "admin");
      session.command("create user fake identified by fake role admin").close();
      Assert.fail("It should not set the password");
    } catch (Exception ex) {
      assertThat(ex).isInstanceOf(OInvalidPasswordException.class);
    }
  }

}
