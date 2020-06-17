package com.orientechnologies.distribution.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/**
 * This integration test keeps track of issues to avoid regressions. It creates a database called as
 * the class name, which is dropped at the end of the work. Created by santo-it on 2017-03-22.
 */
public class OGitHubIssuesIT extends OSingleOrientDBServerWithDatabasePerTestMethodBaseIT {

  @Test
  public void Issue7264() throws Exception {

    final ODatabaseSession db = pool.acquire();
    OClass oOtherClass = db.createVertexClass("OtherClass");

    OProperty oPropertyOtherCI = oOtherClass.createProperty("OtherCI", OType.STRING);
    oPropertyOtherCI.setCollate(OCaseInsensitiveCollate.NAME);

    OProperty oPropertyOtherCS = oOtherClass.createProperty("OtherCS", OType.STRING);
    oPropertyOtherCS.setCollate(ODefaultCollate.NAME);

    oOtherClass.createIndex("other_ci_idx", OClass.INDEX_TYPE.NOTUNIQUE, "OtherCI");
    oOtherClass.createIndex("other_cs_idx", OClass.INDEX_TYPE.NOTUNIQUE, "OtherCS");

    db.command("INSERT INTO OtherClass SET OtherCS='abc', OtherCI='abc';");
    db.command("INSERT INTO OtherClass SET OtherCS='ABC', OtherCI='ABC';");
    db.command("INSERT INTO OtherClass SET OtherCS='Abc', OtherCI='Abc';");
    db.command("INSERT INTO OtherClass SET OtherCS='aBc', OtherCI='aBc';");
    db.command("INSERT INTO OtherClass SET OtherCS='abC', OtherCI='abC';");
    db.command("INSERT INTO OtherClass SET OtherCS='ABc', OtherCI='ABc';");
    db.command("INSERT INTO OtherClass SET OtherCS='aBC', OtherCI='aBC';");
    db.command("INSERT INTO OtherClass SET OtherCS='AbC', OtherCI='AbC';");

    OResultSet results = db.query("SELECT FROM OtherClass WHERE OtherCS='abc'");
    assertThat(results).hasSize(1);
    results = db.query("SELECT FROM OtherClass WHERE OtherCI='abc'");
    assertThat(results).hasSize(8);

    OClass oClassCS = db.createVertexClass("CaseSensitiveCollationIndex");

    OProperty oPropertyGroupCS = oClassCS.createProperty("Group", OType.STRING);
    oPropertyGroupCS.setCollate(ODefaultCollate.NAME);
    OProperty oPropertyNameCS = oClassCS.createProperty("Name", OType.STRING);
    oPropertyNameCS.setCollate(ODefaultCollate.NAME);
    OProperty oPropertyVersionCS = oClassCS.createProperty("Version", OType.STRING);
    oPropertyVersionCS.setCollate(ODefaultCollate.NAME);

    oClassCS.createIndex(
        "group_name_version_cs_idx", OClass.INDEX_TYPE.NOTUNIQUE, "Group", "Name", "Version");

    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='abc', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='ABC', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='Abc', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='aBc', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='abC', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='ABc', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='aBC', Version='1';");
    db.command("INSERT INTO CaseSensitiveCollationIndex SET `Group`='1', Name='AbC', Version='1';");

    results =
        db.query(
            "SELECT FROM CaseSensitiveCollationIndex WHERE Version='1' AND `Group` = '1' AND Name='abc'");
    assertThat(results).hasSize(1);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseSensitiveCollationIndex WHERE Version='1' AND Name='abc' AND `Group` = '1'");
    assertThat(results).hasSize(1);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseSensitiveCollationIndex WHERE `Group` = '1' AND Name='abc' AND Version='1'");
    assertThat(results).hasSize(1);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseSensitiveCollationIndex WHERE `Group` = '1' AND Version='1' AND Name='abc'");
    assertThat(results).hasSize(1);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseSensitiveCollationIndex WHERE Name='abc' AND Version='1' AND `Group` = '1'");
    assertThat(results).hasSize(1);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseSensitiveCollationIndex WHERE Name='abc' AND `Group` = '1' AND Version='1'");
    assertThat(results).hasSize(1);
    results.close();

    OClass oClassCI = db.createVertexClass("CaseInsensitiveCollationIndex");

    OProperty oPropertyGroupCI = oClassCI.createProperty("Group", OType.STRING);
    oPropertyGroupCI.setCollate(OCaseInsensitiveCollate.NAME);
    OProperty oPropertyNameCI = oClassCI.createProperty("Name", OType.STRING);
    oPropertyNameCI.setCollate(OCaseInsensitiveCollate.NAME);
    OProperty oPropertyVersionCI = oClassCI.createProperty("Version", OType.STRING);
    oPropertyVersionCI.setCollate(OCaseInsensitiveCollate.NAME);

    oClassCI.createIndex(
        "group_name_version_ci_idx", OClass.INDEX_TYPE.NOTUNIQUE, "Group", "Name", "Version");

    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='abc', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='ABC', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='Abc', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='aBc', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='abC', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='ABc', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='aBC', Version='1';");
    db.command(
        "INSERT INTO CaseInsensitiveCollationIndex SET `Group`='1', Name='AbC', Version='1';");

    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE Version='1' AND `Group` = '1' AND Name='abc'");
    assertThat(results).hasSize(8);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE Version='1' AND Name='abc' AND `Group` = '1'");
    assertThat(results).hasSize(8);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE `Group` = '1' AND Name='abc' AND Version='1'");

    assertThat(results).hasSize(8);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE `Group` = '1' AND Version='1' AND Name='abc'");
    assertThat(results).hasSize(8);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE Name='abc' AND Version='1' AND `Group` = '1'");
    assertThat(results).hasSize(8);
    results.close();

    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE Name='abc' AND `Group` = '1' AND Version='1'");
    assertThat(results).hasSize(8);
    results.close();

    // test that Group = 1 (integer) is correctly converted to String
    results =
        db.query(
            "SELECT FROM CaseInsensitiveCollationIndex WHERE Name='abc' AND `Group` = 1 AND Version='1'");
    assertThat(results).hasSize(8);
    results.close();
  }

  @Test
  public void Issue7249() throws Exception {
    final ODatabaseSession db = pool.acquire();

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

    OResultSet rs =
        db.query(
            "SELECT in('t7249HasFriend').size() as InFriendsNumber FROM t7249Profiles WHERE Name='Santo'");
    List<OResult> results = rs.stream().collect(Collectors.toList());
    rs.close();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Integer>getProperty("InFriendsNumber")).isEqualTo(1);

    rs =
        db.query(
            "SELECT out('t7249HasFriend').size() as OutFriendsNumber FROM t7249Profiles WHERE Name='Santo'");
    results = rs.stream().collect(Collectors.toList());
    rs.close();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Integer>getProperty("OutFriendsNumber")).isEqualTo(3);

    rs =
        db.query(
            "SELECT both('t7249HasFriend').size() as TotalFriendsNumber FROM t7249Profiles WHERE Name='Santo'");
    results = rs.stream().collect(Collectors.toList());
    rs.close();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Integer>getProperty("TotalFriendsNumber")).isEqualTo(4);
  }

  @Test
  public void Issue7256() throws Exception {

    final ODatabase db = pool.acquire();

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
    db.command(
        "INSERT INTO t7265Restaurants SET Id = 1, Name = 'La Brasserie de Milan', Type = 'restaurant';");
    db.command("INSERT INTO t7265Countries SET Id = 1, Code = 'AD', Name = 'Andorra';");

    db.command(
        "CREATE EDGE t7265HasEaten FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Restaurants WHERE Id=1);");
    db.command(
        "CREATE EDGE t7265HasStayed FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Hotels WHERE Id=1);");
    db.command(
        "CREATE EDGE t7265IsFromCountry FROM (SELECT FROM t7265Customers WHERE OrderedId=1) TO (SELECT FROM t7265Countries WHERE Id=1);");

    OResultSet results =
        db.query(
            "MATCH {class: t7265Customers, as: customer, where: (OrderedId=1)}--{Class: t7265Services, as: service} RETURN service.Name");

    assertThat(results).hasSize(2);
    results.close();
  }
}
