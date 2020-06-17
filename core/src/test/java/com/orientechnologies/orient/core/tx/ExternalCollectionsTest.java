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
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class ExternalCollectionsTest {

  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    db =
        new ODatabaseDocumentTx("memory:" + ExternalCollectionsTest.class.getSimpleName()).create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Ignore // re-enable or remove after decision on #6370 is made
  @Test
  public void testTxCreateTxUpdate() {
    final List<Integer> list = new ArrayList<>();
    list.add(0);

    db.begin();
    final ODocument document = new ODocument();
    document.field("list", list);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);
    db.commit();
    Assert.assertEquals(document.field("list"), list);

    db.begin();
    list.add(1);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);
    db.commit();
    Assert.assertEquals(document.field("list"), list);
  }

  @Ignore // re-enable or remove after decision on #6370 is made
  @Test
  public void testNonTxCreateTxUpdate() {
    final List<Integer> list = new ArrayList<>();
    list.add(0);

    final ODocument document = new ODocument();
    document.field("list", list);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);

    db.begin();
    list.add(1);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);
    db.commit();
    Assert.assertEquals(document.field("list"), list);
  }

  @Ignore // re-enable or remove after decision on #6370 is made
  @Test
  public void testTxCreateNonTxUpdate() {
    final List<Integer> list = new ArrayList<>();
    list.add(0);

    db.begin();
    final ODocument document = new ODocument();
    document.field("list", list);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);
    db.commit();
    Assert.assertEquals(document.field("list"), list);

    list.add(1);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);
  }

  @Ignore // re-enable or remove after decision on #6370 is made
  @Test
  public void testNonTxCreateNonTxUpdate() {
    final List<Integer> list = new ArrayList<>();
    list.add(0);

    final ODocument document = new ODocument();
    document.field("list", list);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);

    list.add(1);
    Assert.assertEquals(document.field("list"), list);
    document.save();
    Assert.assertEquals(document.field("list"), list);
  }
}
