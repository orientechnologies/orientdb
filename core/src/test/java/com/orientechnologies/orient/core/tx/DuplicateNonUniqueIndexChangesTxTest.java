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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class DuplicateNonUniqueIndexChangesTxTest extends BaseMemoryDatabase {

  private OIndex index;

  public void beforeTest() {
    super.beforeTest();
    final OClass class_ = db.getMetadata().getSchema().createClass("Person");
    index =
        class_
            .createProperty("name", OType.STRING)
            .createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
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

    // change some names
    db.save(person3.field("name", "Name3"));

    db.commit();

    // verify index state
    assertRids(null, person1, person2);
    assertRids("Name3", person3);
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
    assertRids(null);
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);

    db.begin();

    // saved persons will have null name
    db.save(person1.field("name", (Object) null));
    db.save(person2.field("name", (Object) null));
    db.save(person3.field("name", (Object) null));

    // change names
    db.save(person1.field("name", "Name2"));
    db.save(person2.field("name", "Name1"));
    db.save(person3.field("name", "Name2"));

    // and again
    db.save(person1.field("name", "Name1"));
    db.save(person2.field("name", "Name2"));

    db.commit();

    // verify index state
    assertRids(null);
    assertRids("Name1", person1);
    assertRids("Name2", person2, person3);
    assertRids("Name3");
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

    // change some names
    db.save(person2.field("name", "Name1"));
    db.save(person2.field("name", "Name2"));
    db.save(person3.field("name", "Name2"));

    db.commit();

    // verify index state
    assertRids("same", person1);
    assertRids("Name1");
    assertRids("Name2", person2, person3);
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
    assertRids(null);
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);

    db.begin();

    // saved persons will have same name
    db.save(person1.field("name", "same"));
    db.save(person2.field("name", "same"));
    db.save(person3.field("name", "same"));

    // change names back to unique in reverse order
    db.save(person3.field("name", "Name3"));
    db.save(person2.field("name", "Name2"));
    db.save(person1.field("name", "Name1"));

    db.commit();

    // verify index state
    assertRids("same");
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);
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
    db.save(person4.field("name", "Name2"));

    db.commit();

    // verify index state
    assertRids("Name1");
    assertRids("Name2", person2, person4);
    assertRids("Name3");
    assertRids("Name4");
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
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);
    assertRids("Name4", person4);

    db.begin();

    db.delete(person1);
    db.save(person2.field("name", "same"));
    db.delete(person3);
    db.save(person4.field("name", "same"));
    db.save(person2.field("name", "Name2"));
    db.save(person4.field("name", "Name2"));

    db.commit();

    // verify index state
    assertRids("same");
    assertRids("Name1");
    assertRids("Name2", person2, person4);
    assertRids("Name3");
    assertRids("Name4");

    db.begin();
    db.delete(person2);
    db.delete(person4);
    db.commit();

    // verify index state
    assertRids("Name2");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testManyManyUpdatesToTheSameKey() {
    final Set<Integer> unseen = new HashSet<Integer>();

    db.begin();
    for (int i = 0; i < OTransactionIndexChangesPerKey.SET_ADD_THRESHOLD * 2; ++i) {
      ODocument pers = db.newInstance("Person");
      pers.field("name", "Name");
      pers.field("serial", i);
      db.save(pers);
      unseen.add(i);
    }
    db.commit();

    // verify index state
    try (Stream<ORID> stream = index.getInternal().getRids("Name")) {
      stream.forEach(
          (rid) -> {
            final ODocument document = db.load(rid);
            unseen.remove(document.<Integer>field("serial"));
          });
    }
    Assert.assertTrue(unseen.isEmpty());
  }

  @SuppressWarnings("unchecked")
  private void assertRids(String indexKey, OIdentifiable... rids) {
    final Set<ORID> actualRids;
    try (Stream<ORID> stream = index.getInternal().getRids(indexKey)) {
      actualRids = stream.collect(Collectors.toSet());
    }
    Assert.assertEquals(actualRids, new HashSet<Object>(Arrays.asList(rids)));
  }
}
