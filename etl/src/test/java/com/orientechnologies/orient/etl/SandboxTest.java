/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.After;
import org.junit.Test;

import java.util.List;

/**
 * Created by frank on 27/09/2016.
 */
public class SandboxTest {

  private ODatabaseDocument db;

  @After
  public void tearDown() throws Exception {
    if (db != null)
      db.drop();

  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldThrowParsingExceptionDueToLackOfClass() throws Exception {

    //Set @class has not the reference to rhe db, it doesn't create the class on it
    ODocument doc = new ODocument().field("@class", "Test").field("name", "myName");

    db = new ODatabaseDocumentTx("memory:testDB").create();

    db.save(doc);

    //where is the doc?
    List<ODocument> res = db.command(new OCommandSQL("SELECT from Test")).execute();

    res.forEach(d -> System.out.println(d.toJSON()));

  }

  @Test
  public void shouldFindClass() throws Exception {

    //the db is created before the doc, so it is on the ThreadLocal

    db = new ODatabaseDocumentTx("memory:testDB").create();

    //the set of field @class creates the class on the db
    ODocument doc = new ODocument().field("@class", "Test").field("name", "myName");

    db.save(doc);
    List<ODocument> res = db.command(new OCommandSQL("SELECT from Test")).execute();

    res.forEach(d -> System.out.println(d.toJSON()));

  }
}
