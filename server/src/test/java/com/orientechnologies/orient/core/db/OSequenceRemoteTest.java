package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.AbstractRemoteTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 19/05/2017.
 */
public class OSequenceRemoteTest extends AbstractRemoteTest {

  ODatabaseDocument db;

  @Override
  public void setup() throws Exception {
    super.setup();

    db = new ODatabaseDocumentTx("remote:localhost/" + name.getMethodName());

    db.open("admin", "admin");

    db.command(new OCommandSQL("CREATE CLASS Person EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE SEQUENCE personIdSequence TYPE ORDERED;")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default \"sequence('personIdSequence').next()\");"))
        .execute();
    db.command(new OCommandSQL("CREATE INDEX Person.id ON Person (id) UNIQUE")).execute();

  }

  @Override
  public void teardown() {
    super.teardown();
    db.close();
  }

  @Test
  public void shouldSequenceWithDefaultValueNoTx() {


    db.getMetadata().reload();

    for (int i = 0; i < 10; i++) {
      db.getMetadata().reload();
      ODocument person = db.newInstance("Person");
      person.field("name", "Foo" + i);
      person.save();
    }

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void shouldSequenceBySQL() {


    db.getMetadata().reload();

    for (int i = 0; i < 10; i++) {
      db.command(new OCommandSQL("INSERT INTO Person set name = 'Foo" +i+"'" )).execute();

    }

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void shouldSequenceWithDefaultValueTx() {


    for (int i = 0; i < 10; i++) {
      db.begin();
      db.getMetadata().reload();
      ODocument person = db.newInstance("Person");
      person.field("name", "Foo" + i);
      person.save();
      db.commit();
    }

    assertThat(db.countClass("Person")).isEqualTo(10);
  }
}
