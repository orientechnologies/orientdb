package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Test;

public class BigDecimalQuerySupportTest extends BaseMemoryDatabase {

  @Test
  public void testDecimalPrecision() throws Exception {

    db.command(new OCommandSQL("CREATE Class Test")).execute();
    db.command(new OCommandSQL("CREATE Property Test.salary DECIMAL")).execute();
    db.command(new OCommandSQL("INSERT INTO Test set salary = ?"))
        .execute(new BigDecimal("179999999999.99999999999999999999"));
    List<ODocument> list = db.query(new OSQLSynchQuery<ODocument>("SELECT * FROM Test"));

    ODocument doc = (ODocument) list.get(0);
    BigDecimal salary = doc.field("salary");

    assertEquals(new BigDecimal("179999999999.99999999999999999999"), salary);
  }
}
