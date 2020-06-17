package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Created by santo-it on 2017-08-27. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationOrdersIT extends OIntegrationTestTemplate {

  @Test
  public void test_Orders_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  count(*) as OrdersNumber, \n"
                + "  sum(Amount) AS TotalRevenuesFromOrders, \n"
                + "  min(Amount) as MinAmount,\n"
                + "  (sum(Amount)/count(*)) as AverageAmount,\n"
                + "  max(Amount) as MaxAmount\n"
                + "FROM Orders");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(1);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("OrdersNumber")).isEqualTo(812);
    assertThat(result.<Long>getProperty("TotalRevenuesFromOrders")).isEqualTo(438635);
    assertThat(result.<Long>getProperty("MinAmount")).isEqualTo(100);
    assertThat(result.<Double>getProperty("AverageAmount")).isEqualTo(540.1908866995074);
    assertThat(result.<Long>getProperty("MaxAmount")).isEqualTo(999);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Orders_Example_2() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  count(*) as OrdersCount, \n"
                + "  OrderDate.format('yyyy') AS OrderYear \n"
                + "FROM Orders \n"
                + "GROUP BY OrderYear \n"
                + "ORDER BY OrdersCount DESC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(7);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("OrdersCount")).isEqualTo(135);
    assertThat(result.<String>getProperty("OrderYear")).isEqualTo("2016");

    resultSet.close();
    db.close();
  }

  // example 3 is handled already in another file

  @Test
  public void test_Orders_Example_4() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  customer.OrderedId as customerOrderedId, \n"
                + "  SUM(order.Amount) as totalAmount \n"
                + "FROM (\n"
                + "  MATCH {Class: Customers, as: customer}<-HasCustomer-{class: Orders, as: order} \n"
                + "  RETURN customer, order\n"
                + ") \n"
                + "GROUP BY customerOrderedId \n"
                + "ORDER BY totalAmount DESC \n"
                + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("customerOrderedId")).isEqualTo(332);
    assertThat(result.<Long>getProperty("totalAmount")).isEqualTo(4578);

    resultSet.close();
    db.close();
  }

  // examples 5 and 6 are handled already in other files

}
