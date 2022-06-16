package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Arrays;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OCompositeIndexGrowShrinkIT {
  private OrientDB orientDB;
  private ODatabaseDocument db;
  private Random random = new Random();

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute(
        " create database ? memory users (admin identified by 'adminpwd' role admin) ",
        OCompositeIndexGrowShrinkIT.class.getSimpleName());
    db = orientDB.open(OCompositeIndexGrowShrinkIT.class.getSimpleName(), "admin", "adminpwd");
  }

  @After
  public void after() {
    db.close();
    orientDB.drop(OCompositeIndexGrowShrinkIT.class.getSimpleName());
    orientDB.close();
  }

  public String randomText() {
    String str = new String();
    int count = random.nextInt(10);
    for (int i = 0; i < count; i++) {
      str += random.nextInt(10000) + " ";
    }
    return str;
  }

  @Test
  public void testCompositeGrowShirnk() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndex");
    clazz.createProperty("id", OType.INTEGER);
    clazz.createProperty("bar", OType.INTEGER);
    clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    clazz.createProperty("name", OType.STRING);

    db.command(
            "create index CompositeIndex_id_tags_name on CompositeIndex (id, tags, name) NOTUNIQUE")
        .close();
    for (int i = 0; i < 150000; i++) {
      OElement rec = db.newElement("CompositeIndex");
      rec.setProperty("id", i);
      rec.setProperty("bar", i);
      rec.setProperty(
          "tags",
          Arrays.asList(
              "soem long and more complex tezxt just un case it may be important", "two"));
      rec.setProperty("name", "name" + i);
      rec.save();
    }
    db.command("delete from CompositeIndex").close();
  }

  @Test
  public void testCompositeGrowDrop() {

    final OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndex");
    clazz.createProperty("id", OType.INTEGER);
    clazz.createProperty("bar", OType.INTEGER);
    clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    clazz.createProperty("name", OType.STRING);

    db.command(
            "create index CompositeIndex_id_tags_name on CompositeIndex (id, tags, name) NOTUNIQUE")
        .close();

    for (int i = 0; i < 150000; i++) {
      OElement rec = db.newElement("CompositeIndex");
      rec.setProperty("id", i);
      rec.setProperty("bar", i);
      rec.setProperty(
          "tags",
          Arrays.asList(
              "soem long and more complex tezxt just un case it may be important", "two"));
      rec.setProperty("name", "name" + i);
      rec.save();
    }
    db.command("drop index CompositeIndex_id_tags_name").close();
  }
}
