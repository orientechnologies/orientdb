/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.tx;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Sergey Sitnikov
 */
public class DuplicateDictionaryIndexChangesTxTest extends BaseMemoryDatabase {

  private OIndex index;

  public void beforeTest() {
    super.beforeTest();
    final OClass class_ = db.getMetadata().getSchema().createClass("Person");
    index =
        class_
            .createProperty("name", OType.STRING)
            .createIndex(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX);
  }

  @Test
  public void testDuplicateNullsOnCreate() {
    db.begin();

    // saved persons will have null name
    final ODocument person1 = db.newInstance("Person");
    db.save(person1);
    final ODocument person2 = db.newInstance("Person");
    db.save(person2);
    final ODocument person3 = db.newInstance("Person");
    db.save(person3);

    // change some names to not null
    person1.field("name", "Name1").save();
    person2.field("name", "Name1").save();

    // should never throw
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("Name1")) {
      Assert.assertEquals(person2, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids(null)) {
      Assert.assertEquals(person3, rids.findFirst().map(ORID::getRecord).orElse(null));
    }
  }

  @Test
  public void testDuplicateNullsOnUpdate() {
    db.begin();
    final ODocument person1 = db.newInstance("Person");
    person1.field("name", (Object) null);
    db.save(person1);
    final ODocument person2 = db.newInstance("Person");
    person2.field("name", (Object) null);
    db.save(person2);
    final ODocument person3 = db.newInstance("Person");
    person3.field("name", (Object) null);
    db.save(person3);
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids(null)) {
      Assert.assertEquals(person3, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    db.begin();

    // change some names
    person1.field("name", "Name2").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

    // and again
    person1.field("name", "Name1").save();
    person2.field("name", "Name1").save();

    // should never throw
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("Name1")) {
      Assert.assertEquals(person2, rids.findFirst().map(ORID::getRecord).orElse(null));
    }
    try (Stream<ORID> rids = index.getInternal().getRids("Name3")) {
      Assert.assertEquals(person3, rids.findFirst().map(ORID::getRecord).orElse(null));
    }
  }

  @Test
  public void testDuplicateValuesOnCreate() {
    db.begin();

    // saved persons will have same name
    final ODocument person1 = db.newInstance("Person");
    person1.field("name", "same");
    db.save(person1);
    final ODocument person2 = db.newInstance("Person");
    person2.field("name", "same");
    db.save(person2);
    final ODocument person3 = db.newInstance("Person");
    person3.field("name", "same");
    person3.save();

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name1").save();

    // should never throw
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("same")) {
      Assert.assertNull(rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name2")) {
      Assert.assertEquals(person2, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> name1 = index.getInternal().getRids("Name1")) {
      Assert.assertEquals(person3, name1.findFirst().map(ORID::getRecord).orElse(null));
    }
  }

  @Test
  public void testDuplicateValuesOnUpdate() {
    db.begin();
    final ODocument person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    final ODocument person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    final ODocument person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("Name1")) {
      Assert.assertEquals(person1, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name2")) {
      Assert.assertEquals(person2, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name3")) {
      Assert.assertEquals(person3, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    db.begin();

    // saved persons will have same name
    person1.field("name", "same").save();
    person2.field("name", "same").save();
    person3.field("name", "same").save();

    // change names back to unique in reverse order
    person3.field("name", "Name3").save();
    person2.field("name", "Name2").save();
    person1.field("name", "Name1").save();

    // should never throw
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("same")) {
      Assert.assertNull(rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name1")) {
      Assert.assertEquals(person1, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name2")) {
      Assert.assertEquals(person2, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name3")) {
      Assert.assertEquals(person3, rids.findFirst().map(ORID::getRecord).orElse(null));
    }
  }

  @Test
  public void testDuplicateValuesOnCreateDelete() {
    db.begin();

    // saved persons will have same name
    final ODocument person1 = db.newInstance("Person");
    person1.field("name", "same");
    db.save(person1);
    final ODocument person2 = db.newInstance("Person");
    person2.field("name", "same");
    db.save(person2);
    final ODocument person3 = db.newInstance("Person");
    person3.field("name", "same");
    db.save(person3);
    final ODocument person4 = db.newInstance("Person");
    person4.field("name", "same");
    db.save(person4);

    person1.delete();
    person2.field("name", "Name2").save();
    person3.delete();
    person4.field("name", "Name2").save();
    person4.delete();

    // should never throw
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("same")) {
      Assert.assertNull(rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    try (Stream<ORID> rids = index.getInternal().getRids("Name2")) {
      Assert.assertEquals(person2, rids.findFirst().map(ORID::getRecord).orElse(null));
    }
  }

  @Test
  public void testDuplicateValuesOnUpdateDelete() {
    db.begin();
    final ODocument person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    final ODocument person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    final ODocument person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    final ODocument person4 = db.newInstance("Person");
    person4.field("name", "Name4");
    db.save(person4);
    db.commit();

    // verify index state
    try (Stream<ORID> rids = index.getInternal().getRids("Name1")) {
      Assert.assertEquals(person1, rids.findFirst().map(ORID::getRecord).orElse(null));
    }

    Assert.assertEquals(person2, getDocumentByKey("Name2"));
    Assert.assertEquals(person3, getDocumentByKey("Name3"));
    Assert.assertEquals(person4, getDocumentByKey("Name4"));

    db.begin();

    person1.delete();
    person2.field("name", "same").save();
    person3.delete();
    person4.field("name", "same").save();
    person2.field("name", "Name2").save();
    person4.field("name", "Name2").save();

    // should never throw
    db.commit();

    // verify index state
    Assert.assertEquals(person4, getDocumentByKey("Name2"));
    Assert.assertNull(getDocumentByKey("same"));

    db.begin();
    person2.delete();
    person4.delete();
    db.commit();

    // verify index state
    Assert.assertNull(getDocumentByKey("Name2"));
    Assert.assertNull(getDocumentByKey("same"));
  }

  private ODocument getDocumentByKey(String key) {
    try (Stream<ORID> rids = index.getInternal().getRids(key)) {
      return (ODocument) rids.findFirst().map(ORID::getRecord).orElse(null);
    }
  }
}
