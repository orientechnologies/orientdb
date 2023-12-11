package com.orientechnologies.orient.core.sql.orderby;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;

public class TestOrderBy extends BaseMemoryDatabase {

  @Test
  public void testGermanOrderBy() {
    db.set(ATTRIBUTES.LOCALECOUNTRY, Locale.GERMANY.getCountry());
    db.set(ATTRIBUTES.LOCALELANGUAGE, Locale.GERMANY.getLanguage());
    db.getMetadata().getSchema().createClass("test");
    ORecord res1 = db.save(new ODocument("test").field("name", "Ähhhh"));
    ORecord res2 = db.save(new ODocument("test").field("name", "Ahhhh"));
    ORecord res3 = db.save(new ODocument("test").field("name", "Zebra"));
    List<OResult> queryRes =
        db.query("select from test order by name").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0).getIdentity().get(), res2.getIdentity());
    assertEquals(queryRes.get(1).getIdentity().get(), res1.getIdentity());
    assertEquals(queryRes.get(2).getIdentity().get(), res3.getIdentity());

    queryRes =
        db.query("select from test order by name desc ").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0).getIdentity().get(), res3.getIdentity());
    assertEquals(queryRes.get(1).getIdentity().get(), res1.getIdentity());
    assertEquals(queryRes.get(2).getIdentity().get(), res2.getIdentity());
  }

  @Test
  @Ignore
  public void testGermanOrderByIndex() {
    db.set(ATTRIBUTES.LOCALECOUNTRY, Locale.GERMANY.getCountry());
    db.set(ATTRIBUTES.LOCALELANGUAGE, Locale.GERMANY.getLanguage());
    OClass clazz = db.getMetadata().getSchema().createClass("test");
    clazz.createProperty("name", OType.STRING).createIndex(INDEX_TYPE.NOTUNIQUE);
    ORecord res1 = db.save(new ODocument("test").field("name", "Ähhhh"));
    ORecord res2 = db.save(new ODocument("test").field("name", "Ahhhh"));
    ORecord res3 = db.save(new ODocument("test").field("name", "Zebra"));
    List<OResult> queryRes =
        db.query("select from test order by name").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0).getIdentity().get(), res2.getIdentity());
    assertEquals(queryRes.get(1).getIdentity().get(), res1.getIdentity());
    assertEquals(queryRes.get(2).getIdentity().get(), res3.getIdentity());

    queryRes =
        db.query("select from test order by name desc ").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0), res3);
    assertEquals(queryRes.get(1), res1);
    assertEquals(queryRes.get(2), res2);
  }
}
