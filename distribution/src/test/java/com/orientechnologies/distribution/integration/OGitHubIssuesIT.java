package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Created by santo-it on 03/22/2017.
 */
public class OGitHubIssuesIT extends OIntegrationTestTemplate {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    String database = this.getClass().getName();
    if (!orientDB.exists(database)) {
      orientDB.create(database, ODatabaseType.PLOCAL);
    }

    db = orientDB.open(database, "admin", "admin");
  }

  @Test
  public void Issue7249() throws Exception {

    db.command("CREATE CLASS t7249Profiles EXTENDS V;");
    db.command("CREATE CLASS t7249HasFriend EXTENDS E;");

    db.command("INSERT INTO t7249Profiles SET Name = 'Santo';");
    db.command("INSERT INTO t7249Profiles SET Name = 'Luca';");
    db.command("INSERT INTO t7249Profiles SET Name = 'Luigi';");
    db.command("INSERT INTO t7249Profiles SET Name = 'Colin';");
    db.command("INSERT INTO t7249Profiles SET Name = 'Enrico';");

    db.command(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Santo') TO (SELECT FROM t7249Profiles WHERE Name='Luca');");
    db.command(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Santo') TO (SELECT FROM t7249Profiles WHERE Name='Luigi');");
    db.command(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Santo') TO (SELECT FROM t7249Profiles WHERE Name='Colin');");
    db.command(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Enrico') TO (SELECT FROM t7249Profiles WHERE Name='Santo');");

    List<OResult> results = db.query("SELECT in('t7249HasFriend').size() as InFriendsNumber FROM t7249Profiles WHERE Name='Santo'")
        .stream().collect(Collectors.toList());
    assertEquals(1, results.size());
    assertEquals((Object) 1, results.get(0).getProperty("InFriendsNumber"));

    results = db.query("SELECT out('t7249HasFriend').size() as OutFriendsNumber FROM t7249Profiles WHERE Name='Santo'").stream()
        .collect(Collectors.toList());
    assertEquals(1, results.size());
    assertEquals((Object) 3, results.get(0).getProperty("OutFriendsNumber"));

    results = db.query("SELECT both('t7249HasFriend').size() as TotalFriendsNumber FROM t7249Profiles WHERE Name='Santo'").stream()
        .collect(Collectors.toList());
    assertEquals(1, results.size());
    assertEquals((Object) 4, results.get(0).getProperty("TotalFriendsNumber"));

  }

  @Test
  public void Issue7256() throws Exception {

    db.command("CREATE CLASS t7265Customers EXTENDS V;");
    db.command("CREATE CLASS t7265Services EXTENDS V;");
    db.command("CREATE CLASS t7265Hotels EXTENDS V, t7265Services;");
    db.command("CREATE CLASS t7265Restaurants EXTENDS V, t7265Services;");
    db.command("CREATE CLASS t7265Countries EXTENDS V;");

    db.command("CREATE CLASS t7265IsFromCountry EXTENDS E;");
    db.command("CREATE CLASS t7265HasUsedService EXTENDS E;");
    db.command("CREATE CLASS t7265HasStayed EXTENDS E, t7265HasUsedService;");
    db.command("CREATE CLASS t7265HasEaten EXTENDS E, t7265HasUsedService;");

    db.command("INSERT INTO t7265Customers SET OrderedId = 1, Phone = '+1400844724';");
    db.command("INSERT INTO t7265Hotels SET Id = 1, Name = 'Best Western Ascott', Type = 'hotel';");
    db.command("INSERT INTO t7265Restaurants SET Id = 1, Name = 'La Brasserie de Milan', Type = 'restaurant';");
    db.command("INSERT INTO t7265Countries SET Id = 1, Code = 'AD', Name = 'Andorra';");

    db.command(
        "CREATE EDGE t7265HasEaten FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Restaurants WHERE Id=1);");
    db.command(
        "CREATE EDGE t7265HasStayed FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Hotels WHERE Id=1);");
    db.command(
        "CREATE EDGE t7265IsFromCountry FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Countries WHERE Id=1);");

    OResultSet results = db.query(
        "MATCH {class: t7265Customers, as: customer, where: (OrderedId=1)}--{Class: t7265Services, as: service} RETURN service.Name");

    assertThat(results).hasSize(2);
  }

}
