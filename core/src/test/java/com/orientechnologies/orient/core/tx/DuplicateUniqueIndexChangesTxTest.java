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
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class DuplicateUniqueIndexChangesTxTest extends BaseMemoryDatabase {

  private OIndex index;

  public void beforeTest() {
    super.beforeTest();
    final OClass class_ = db.getMetadata().getSchema().createClass("Person");
    index =
        class_
            .createProperty("name", OType.STRING)
            .createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
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

    // change names to unique
    db.save(person1.field("name", "Name1"));
    db.save(person2.field("name", "Name2"));
    db.save(person3.field("name", "Name3"));

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  private ODocument fetchDocumentFromIndex(String o) {
    try (Stream<ORID> stream = index.getInternal().getRids(o)) {
      return (ODocument) stream.findFirst().map(ORID::getRecord).orElse(null);
    }
  }

  @Test
  public void testDuplicateNullsOnUpdate() {
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
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    db.begin();

    // saved persons will have null name
    db.save(person1.field("name", (Object) null));
    db.save(person2.field("name", (Object) null));
    db.save(person3.field("name", (Object) null));

    // change names back to unique swapped
    db.save(person1.field("name", "Name2"));
    db.save(person2.field("name", "Name1"));
    db.save(person3.field("name", "Name3"));

    // and again
    db.save(person1.field("name", "Name1"));
    db.save(person2.field("name", "Name2"));

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
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
    db.save(person3);

    // change names to unique
    db.save(person1.field("name", "Name1"));
    db.save(person2.field("name", "Name2"));
    db.save(person3.field("name", "Name3"));

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("same"));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
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
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    db.begin();

    // saved persons will have same name
    db.save(person1.field("name", "same"));
    db.save(person2.field("name", "same"));
    db.save(person3.field("name", "same"));

    // change names back to unique in reverse order
    db.save(person3.field("name", "Name3"));
    db.save(person2.field("name", "Name2"));
    db.save(person1.field("name", "Name1"));

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("same"));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
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

    db.delete(person1);
    db.save(person2.field("name", "Name2"));
    db.delete(person3);

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));
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
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    db.begin();

    db.delete(person1);
    db.save(person2.field("name", "same"));
    db.delete(person3);
    db.save(person4.field("name", "same"));
    db.save(person2.field("name", "Name2"));

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));

    db.begin();
    db.delete(person2);
    db.delete(person4);
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("Name2"));
    Assert.assertNull(fetchDocumentFromIndex("same"));
  }

  @Test(expected = ORecordDuplicatedException.class)
  public void testDuplicateCreateThrows() {
    db.begin();
    ODocument person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    ODocument person2 = db.newInstance("Person");
    db.save(person2);
    ODocument person3 = db.newInstance("Person");
    db.save(person3);
    ODocument person4 = db.newInstance("Person");
    person4.field("name", "Name1");
    db.save(person4);
    //    Assert.assertThrows(ORecordDuplicatedException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        db.commit();
    //      }
    //    });
    db.commit();
  }

  @Test(expected = ORecordDuplicatedException.class)
  public void testDuplicateUpdateThrows() {
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
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    db.begin();
    db.save(person1.field("name", "Name1"));
    db.save(person2.field("name", (Object) null));
    db.save(person3.field("name", "Name1"));
    db.save(person4.field("name", (Object) null));
    //    Assert.assertThrows(ORecordDuplicatedException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        db.commit();
    //      }
    //    });
    db.commit();
  }
}
