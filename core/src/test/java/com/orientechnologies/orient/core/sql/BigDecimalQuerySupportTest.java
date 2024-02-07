package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.math.BigDecimal;
import org.junit.Test;

public class BigDecimalQuerySupportTest extends BaseMemoryDatabase {

  @Test
  public void testDecimalPrecision() throws Exception {
    db.command("CREATE Class Test").close();
    db.command("CREATE Property Test.salary DECIMAL").close();
    db.command(
            "INSERT INTO Test set salary = ?", new BigDecimal("179999999999.99999999999999999999"))
        .close();
    try (OResultSet result = db.query("SELECT * FROM Test")) {
      BigDecimal salary = result.next().getProperty("salary");
      assertEquals(new BigDecimal("179999999999.99999999999999999999"), salary);
    }
  }
}
