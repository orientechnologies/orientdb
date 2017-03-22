package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Created by santo-it on 03/22/2017.
 */
public class OGitHubIssuesIT extends OIntegrationTestTemplate {

  @Test
  public void Issue7249() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS t7249Profiles EXTENDS V;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7249HasFriend EXTENDS E;")).execute();

    db.command(new OCommandSQL("INSERT INTO t7249Profiles SET Name = 'Santo';")).execute();
    db.command(new OCommandSQL("INSERT INTO t7249Profiles SET Name = 'Luca';")).execute();
    db.command(new OCommandSQL("INSERT INTO t7249Profiles SET Name = 'Luigi';")).execute();
    db.command(new OCommandSQL("INSERT INTO t7249Profiles SET Name = 'Colin';")).execute();
    db.command(new OCommandSQL("INSERT INTO t7249Profiles SET Name = 'Enrico';")).execute();

    db.command(new OCommandSQL(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Santo') TO (SELECT FROM t7249Profiles WHERE Name='Luca');"))
        .execute();
    db.command(new OCommandSQL(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Santo') TO (SELECT FROM t7249Profiles WHERE Name='Luigi');"))
        .execute();
    db.command(new OCommandSQL(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Santo') TO (SELECT FROM t7249Profiles WHERE Name='Colin');"))
        .execute();
    db.command(new OCommandSQL(
        "CREATE EDGE t7249HasFriend FROM (SELECT FROM t7249Profiles WHERE Name='Enrico') TO (SELECT FROM t7249Profiles WHERE Name='Santo');"))
        .execute();

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

    db.command(new OCommandSQL("CREATE CLASS t7265Customers EXTENDS V;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265Services EXTENDS V;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265Hotels EXTENDS V, t7265Services;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265Restaurants EXTENDS V, t7265Services;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265Countries EXTENDS V;")).execute();

    db.command(new OCommandSQL("CREATE CLASS t7265IsFromCountry EXTENDS E;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265HasUsedService EXTENDS E;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265HasStayed EXTENDS E, t7265HasUsedService;")).execute();
    db.command(new OCommandSQL("CREATE CLASS t7265HasEaten EXTENDS E, t7265HasUsedService;")).execute();

    db.command(new OCommandSQL("INSERT INTO t7265Customers SET OrderedId = 1, Phone = '+1400844724';")).execute();
    db.command(new OCommandSQL("INSERT INTO t7265Hotels SET Id = 1, Name = 'Best Western Ascott', Type = 'hotel';")).execute();
    db.command(new OCommandSQL("INSERT INTO t7265Restaurants SET Id = 1, Name = 'La Brasserie de Milan', Type = 'restaurant';"))
        .execute();
    db.command(new OCommandSQL("INSERT INTO t7265Countries SET Id = 1, Code = 'AD', Name = 'Andorra';")).execute();

    db.command(new OCommandSQL(
        "CREATE EDGE t7265HasEaten FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Restaurants WHERE Id=1);"))
        .execute();
    db.command(new OCommandSQL(
        "CREATE EDGE t7265HasStayed FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Hotels WHERE Id=1);"))
        .execute();
    db.command(new OCommandSQL(
        "CREATE EDGE t7265IsFromCountry FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Countries WHERE Id=1);"))
        .execute();

    List<OResult> results = db.query(
        "MATCH {class: t7265Customers, as: customer, where: (OrderedId=1)}--{Class: t7265Services, as: service} RETURN service.Name")
        .stream().collect(Collectors.toList());
    assertEquals(2, results.size());

  }

}
