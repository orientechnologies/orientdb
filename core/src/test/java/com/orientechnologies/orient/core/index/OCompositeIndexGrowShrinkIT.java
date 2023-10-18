package com.orientechnologies.orient.core.index;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

public class OCompositeIndexGrowShrinkIT extends BaseMemoryDatabase {
  private Random random = new Random();

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
