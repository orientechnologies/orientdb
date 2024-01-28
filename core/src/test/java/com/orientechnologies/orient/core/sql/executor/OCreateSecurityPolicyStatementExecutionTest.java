package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateSecurityPolicyStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPlain() {
    OResultSet result = db.command("CREATE SECURITY POLICY foo");
    result.close();
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNotNull(security.getSecurityPolicy((ODatabaseSession) db, "foo"));
  }
}
