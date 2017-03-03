package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.document.DateTools;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 13/12/2016.
 */
public class OLuceneRangeTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    OSchema schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Person");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("age", OType.INTEGER);

    List<String> names = Arrays.asList("John", "Robert", "Jane", "andrew", "Scott", "luke", "Enriquez", "Luis", "Gabriel", "Sara");
    for (int i = 0; i < 10; i++) {
      db.save(new ODocument("Person")
          .field("name", names.get(i))
          .field("surname", "Reese")
          //from today back one day a time
          .field("date", System.currentTimeMillis() - (i * 3600 * 24 * 1000))
          .field("age", i));
    }

  }

  @Test
  public void shouldUseRangeQueryOnSingleIntegerField() throws Exception {

    db.command("create index Person.age on Person(age) FULLTEXT ENGINE LUCENE");

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.age").getSize()).isEqualTo(10);

    //range
    OResultSet results = db.command("SELECT FROM Person WHERE search_class('age:[5 TO 6]') = true");

    assertThat(results).hasSize(2);

    //single value
    results = db.command("SELECT FROM Person WHERE search_class('age:5') = true");

    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldUseRangeQueryOnSingleDateField() throws Exception {

    db.commit();
    db.command(new OCommandSQL("create index Person.date on Person(date) FULLTEXT ENGINE LUCENE")).execute();
    db.commit();

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.date").getSize()).isEqualTo(10);

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo = DateTools.timeToString(System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    //range
    OResultSet results = db
        .command("SELECT FROM Person WHERE search_class('date:[" + fiveDaysAgo + " TO " + today + "]')=true");

    assertThat(results).hasSize(5);

  }

  @Test
  public void shouldUseRangeQueryMultipleField() throws Exception {

    db.command(new OCommandSQL("create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE LUCENE")).execute();

    assertThat(db.getMetadata().getIndexManager().getIndex("Person.composite").getSize()).isEqualTo(10);

    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo = DateTools.timeToString(System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    //name and age range
    OResultSet results = db
        .command("SELECT * FROM Person WHERE search_class('age:[5 TO 6] name:robert  ')=true");

    assertThat(results).hasSize(3);

    //date range
    results = db.command(
        "SELECT FROM Person WHERE search_class('date:[" + fiveDaysAgo + " TO " + today + "]')=true");

    assertThat(results).hasSize(5);

    //age and date range with MUST
    results = db
        .command("SELECT FROM Person WHERE search_class('+age:[4 TO 7]  +date:[" + fiveDaysAgo + " TO " + today + "]')=true");

    assertThat(results).hasSize(2);

  }

}
