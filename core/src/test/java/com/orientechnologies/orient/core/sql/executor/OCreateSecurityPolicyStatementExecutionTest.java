package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import org.junit.*;

import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateSecurityPolicyStatementExecutionTest {
  static OrientDB orient;
  private ODatabaseSession db;

  @BeforeClass
  public static void beforeClass() {
    orient = new OrientDB("plocal:.", OrientDBConfig.defaultConfig());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    orient.create("test", ODatabaseType.MEMORY);
    this.db = orient.open("test", "admin", "admin");
  }

  @After
  public void after() {
    this.db.close();
    orient.drop("test");
    this.db = null;
  }


  @Test
  public void testPlain() {
    OResultSet result = db.command("CREATE SECURITY POLICY foo");
    result.close();
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNotNull(security.getSecurityPolicy((ODatabaseSession) db, "foo"));
  }


}
