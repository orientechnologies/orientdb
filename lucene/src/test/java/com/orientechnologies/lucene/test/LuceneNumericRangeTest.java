package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;

import static org.assertj.core.api.Assertions.*;

/**
 * Created by frank on 13/12/2016.
 */
public class LuceneNumericRangeTest extends BaseLuceneTest {

  @Test
  public void shouldUseRangeQuery() throws Exception {

    OSchemaProxy schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Person");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("age", OType.LONG);

    db.command(new OCommandSQL("create index Person.age on Person(age) FULLTEXT ENGINE LUCENE")).execute();

    for (int i = 0; i < 10; i++) {
      db.save(new ODocument("Reuse")
          .field("name", "John")
          .field("date", new Date())
          .field("surname", "Reese")
          .field("age", i));
    }

    long size = db.getMetadata().getIndexManager().getIndex("Person.age").getSize();
    System.out.println("size = " + size);
    Collection<ODocument> results = db.command(new OCommandSQL("SELECT FROM Person WHERE age LUCENE '[5 TO 6]'"))
        .execute();

//    assertThat(results).hasSize(2);
  }
}
