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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class DuplicateUniqueIndexChangesTxTest {

  private static ODatabaseDocumentTx db;
  private OIndex index;

  @BeforeClass
  public static void before() {
    db =
        new ODatabaseDocumentTx(
            "memory:" + DuplicateUniqueIndexChangesTxTest.class.getSimpleName());
  }

  @AfterClass
  public static void after() {
    db.drop();
  }

  @Before
  public void beforeMethod() {
    if (!db.isClosed()) db.drop();
    db.create();
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
    final ODocument person1 = db.newInstance("Person").save();
    final ODocument person2 = db.newInstance("Person").save();
    final ODocument person3 = db.newInstance("Person").save();

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

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
    final ODocument person1 = db.newInstance("Person").field("name", "Name1").save();
    final ODocument person2 = db.newInstance("Person").field("name", "Name2").save();
    final ODocument person3 = db.newInstance("Person").field("name", "Name3").save();
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    db.begin();

    // saved persons will have null name
    person1.field("name", (Object) null).save();
    person2.field("name", (Object) null).save();
    person3.field("name", (Object) null).save();

    // change names back to unique swapped
    person1.field("name", "Name2").save();
    person2.field("name", "Name1").save();
    person3.field("name", "Name3").save();

    // and again
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();

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
    final ODocument person1 = db.newInstance("Person").field("name", "same").save();
    final ODocument person2 = db.newInstance("Person").field("name", "same").save();
    final ODocument person3 = db.newInstance("Person").field("name", "same").save();

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

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
    final ODocument person1 = db.newInstance("Person").field("name", "Name1").save();
    final ODocument person2 = db.newInstance("Person").field("name", "Name2").save();
    final ODocument person3 = db.newInstance("Person").field("name", "Name3").save();
    db.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    db.begin();

    // saved persons will have same name
    person1.field("name", "same").save();
    person2.field("name", "same").save();
    person3.field("name", "same").save();

    // change names back to unique in reverse order
    person3.field("name", "Name3").save();
    person2.field("name", "Name2").save();
    person1.field("name", "Name1").save();

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
    final ODocument person1 = db.newInstance("Person").field("name", "same").save();
    final ODocument person2 = db.newInstance("Person").field("name", "same").save();
    final ODocument person3 = db.newInstance("Person").field("name", "same").save();
    final ODocument person4 = db.newInstance("Person").field("name", "same").save();

    person1.delete();
    person2.field("name", "Name2").save();
    person3.delete();

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));
  }

  @Test
  public void testDuplicateValuesOnUpdateDelete() {
    db.begin();
    final ODocument person1 = db.newInstance("Person").field("name", "Name1").save();
    final ODocument person2 = db.newInstance("Person").field("name", "Name2").save();
    final ODocument person3 = db.newInstance("Person").field("name", "Name3").save();
    final ODocument person4 = db.newInstance("Person").field("name", "Name4").save();
    db.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    db.begin();

    person1.delete();
    person2.field("name", "same").save();
    person3.delete();
    person4.field("name", "same").save();
    person2.field("name", "Name2").save();

    // should not throw ORecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));

    db.begin();
    person2.delete();
    person4.delete();
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("Name2"));
    Assert.assertNull(fetchDocumentFromIndex("same"));
  }

  @Test(expected = ORecordDuplicatedException.class)
  public void testDuplicateCreateThrows() {
    db.begin();
    db.newInstance("Person").field("name", "Name1").save();
    db.newInstance("Person").save();
    db.newInstance("Person").save();
    db.newInstance("Person").field("name", "Name1").save();
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
    final ODocument person1 = db.newInstance("Person").field("name", "Name1").save();
    final ODocument person2 = db.newInstance("Person").field("name", "Name2").save();
    final ODocument person3 = db.newInstance("Person").field("name", "Name3").save();
    final ODocument person4 = db.newInstance("Person").field("name", "Name4").save();
    db.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    db.begin();
    person1.field("name", "Name1").save();
    person2.field("name", (Object) null).save();
    person3.field("name", "Name1").save();
    person4.field("name", (Object) null).save();
    //    Assert.assertThrows(ORecordDuplicatedException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        db.commit();
    //      }
    //    });
    db.commit();
  }
}
