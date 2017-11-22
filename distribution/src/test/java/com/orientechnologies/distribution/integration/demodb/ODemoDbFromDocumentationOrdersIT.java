package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by santo-it on 2017-08-27.
 */
@Test
public class ODemoDbFromDocumentationOrdersIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void test_Orders_Example_1() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  count(*) as OrdersNumber, \n" + "  sum(Amount) AS TotalRevenuesFromOrders, \n"
        + "  min(Amount) as MinAmount,\n" + "  (sum(Amount)/count(*)) as AverageAmount,\n" + "  max(Amount) as MaxAmount\n"
        + "FROM Orders");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 1);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Long>getProperty("OrdersNumber"), Long.valueOf(812));
    Assert.assertEquals(result.<Long>getProperty("TotalRevenuesFromOrders"), Long.valueOf(438635));
    Assert.assertEquals(result.<Long>getProperty("MinAmount"), Long.valueOf(100));
    Assert.assertEquals(result.<Double>getProperty("AverageAmount"), 540.1908866995074);
    Assert.assertEquals(result.<Long>getProperty("MaxAmount"), Long.valueOf(999));

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_Orders_Example_2() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  count(*) as OrdersCount, \n" + "  OrderDate.format('yyyy') AS OrderYear \n" + "FROM Orders \n"
            + "GROUP BY OrderYear \n" + "ORDER BY OrdersCount DESC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 7);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Long>getProperty("OrdersCount"), Long.valueOf(135));
    Assert.assertEquals(result.getProperty("OrderYear"), "2016");

    resultSet.close();
  }

  // example 3 is handled already in another file

  @Test(priority = 3)
  public void test_Orders_Example_4() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  customer.OrderedId as customerOrderedId, \n" + "  SUM(order.Amount) as totalAmount \n" + "FROM (\n"
            + "  MATCH {Class: Customers, as: customer}<-HasCustomer-{class: Orders, as: order} \n" + "  RETURN customer, order\n"
            + ") \n" + "GROUP BY customerOrderedId \n" + "ORDER BY totalAmount DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Long>getProperty("customerOrderedId"), Long.valueOf(332));
    Assert.assertEquals(result.<Long>getProperty("totalAmount"), Long.valueOf(4578));

    resultSet.close();
  }

  // examples 5 and 6 are handled already in other files

}
