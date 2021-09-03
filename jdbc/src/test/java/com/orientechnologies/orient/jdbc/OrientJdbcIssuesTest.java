package com.orientechnologies.orient.jdbc;

import java.sql.Statement;
import java.util.HashMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Created by frank on 11/04/2017. */
public class OrientJdbcIssuesTest extends OrientJdbcDbPerMethodTemplateTest {

  @Test
  public void shouldMapNullValues_ph8555() throws Exception {

    String commands =
        "CREATE CLASS Demo;\n"
            //        + "CREATE PROPERTY Demo.firstName STRING\n"
            //        + "CREATE PROPERTY Demo.lastName STRING\n"
            //        + "CREATE PROPERTY Demo.address STRING\n"
            //        + "CREATE PROPERTY Demo.amount INTEGER\n"
            + "INSERT INTO Demo(firstName, lastName, address, amount) VALUES (\"John\", \"John\", \"Street1\", 1234);\n"
            + "INSERT INTO Demo(firstName, lastName, amount) VALUES (\"Lars\", \"Lar\", 2232);\n"
            + "INSERT INTO Demo(firstName, amount) VALUES (\"Lars\", 2232);";

    Statement stmt = conn.createStatement();
    stmt.addBatch("CREATE CLASS Demo;");
    stmt.addBatch(
        "INSERT INTO Demo(firstName, lastName, address, amount) VALUES (\"John\", \"John\", \"Street1\", 1234);");
    stmt.addBatch(
        "INSERT INTO Demo(firstName, lastName, amount) VALUES (\"Lars\", \"Lar\", 2232);");
    stmt.addBatch("INSERT INTO Demo(firstName, amount) VALUES (\"Lars\", 2232);");
    stmt.executeBatch();
    stmt.close();

    stmt = conn.createStatement();
    OrientJdbcResultSet resSet =
        (OrientJdbcResultSet)
            stmt.executeQuery("select firstName , lastName , address, amount from Demo");

    while (resSet.next()) {
      HashMap<String, Object> item = new HashMap<String, Object>();

      int numCols = resSet.getMetaData().getColumnCount();

      for (int i = 1; i <= numCols; i++) {
        String colName = resSet.getMetaData().getColumnName(i);
        Object value = resSet.getObject(colName);
        item.put(colName, value);
      }

      Assertions.assertThat(item).containsKeys("firstName", "lastName", "address", "amount");
    }

    /**
     * Expected: [{firstName=John, lastName=John, amount=1234, address=Street1}, {firstName=Lars,
     * lastName=Lar, amount=2232}, {firstName=Lars, amount=2232}] Result: [{firstName=John,
     * lastName=John, amount=1234, address=Street1}, {firstName=Lars, lastName=Lar, address=null},
     * {firstName=Lars, lastName=null}]
     */
  }
}
