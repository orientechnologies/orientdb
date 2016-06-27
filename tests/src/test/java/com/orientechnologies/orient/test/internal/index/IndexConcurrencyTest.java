/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Steven Thomer
 * @since 03.06.12
 */
class PersonTree {
  public ODocument            root;
  public Map<ORID, ODocument> elements = new HashMap<ORID, ODocument>();

  ODocument AddRoot(String name, String identifier) {
    root = new ODocument("Person").field("name", name).field("identifier", identifier).field("in", new HashSet<ODocument>())
        .field("out", new HashSet<ODocument>());
    root.save();
    elements.put(root.getIdentity(), root);
    return root;
  }

  void SetRoot(ODocument doc) {
    root = doc;
    root.save();
    elements.put(root.getIdentity(), root);
  }

  ODocument AddChild(ODocument parent, String name, String identifier) {
    ODocument child = new ODocument("Person").field("name", name).field("identifier", identifier)
        .field("in", new HashSet<ODocument>()).field("out", new HashSet<ODocument>());
    child.save();
    elements.put(child.getIdentity(), child);
    ODocument edge = new ODocument("E").field("in", parent.getIdentity()).field("out", child.getIdentity());
    edge.save();
    elements.put(edge.getIdentity(), edge);
    child.<Collection<ODocument>> field("in").add(edge);
    parent.<Collection<ODocument>> field("out").add(edge);
    return child;
  }
}

public class IndexConcurrencyTest {
  static final int    subnodes = 3;
  static final int    depth    = 3;

  static final String url      = "remote:localhost/demo";

  public static boolean checkIndexConsistency(ODatabaseDocumentTx db) {
    Map<String, ODocument> persons = new HashMap<String, ODocument>();
    Map<String, ORID> indexPersons = new HashMap<String, ORID>();

    final List<ODocument> result = db.command(new OCommandSQL("select from cluster:Person")).execute();
    for (ODocument d : result) {
      persons.put((String) d.field("name"), d);
    }
    final List<ODocument> indexResult = db.command(new OCommandSQL("select from index:Person.name")).execute();
    for (ODocument d : indexResult) {
      d.setLazyLoad(false);
      indexPersons.put((String) d.field("key"), (ORID) d.field("rid"));
    }

    System.out.println("PersonCount: " + persons.size() + ", IndexCount: " + indexPersons.size());

    boolean missing = false;
    for (Map.Entry<String, ODocument> e : persons.entrySet()) {
      if (!indexPersons.containsKey(e.getKey())) {
        System.out.println("Key found in cluster but not in index: " + e.getValue() + " key: " + e.getKey());
        missing = true;
      }
    }
    for (Map.Entry<String, ORID> e : indexPersons.entrySet()) {
      if (!persons.containsKey(e.getKey())) {
        System.out.println("Key found in index but not in cluster: " + e.getValue() + " key: " + e.getKey());
        missing = true;
      }
    }

    return persons.size() == indexPersons.size() && !missing;
  }

  public static void buildTree(PersonTree tree, ORID rid, String name, int childCount, int level, char startLetter) {
    if (level == 0)
      return;

    for (int i = 0; i < childCount; i++) {
      String newName = name;
      newName += Character.toString((char) (startLetter + i));
      StringBuilder newIdentifier = new StringBuilder(newName);
      newIdentifier.setCharAt(0, 'B');
      ODocument child = tree.AddChild(tree.elements.get(rid), newName, newIdentifier.toString());
      buildTree(tree, child.getIdentity(), newName, childCount, level - 1, startLetter);
    }
  }

  public static void addSubTree(String parentName, char startLetter) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");

    for (int i = 0;; ++i) {
      try {
        ODocument parent;
        final List<ODocument> result = db.command(new OCommandSQL("select from Person where name = '" + parentName + "'"))
            .execute();
        parent = result.get(0);
        if (parent == null) {
          db.close();
          return;
        }

        String newName = parentName;
        newName += Character.toString(startLetter);
        StringBuilder newIdentifier = new StringBuilder(newName);
        newIdentifier.setCharAt(0, 'B');

        db.begin();

        PersonTree tree = new PersonTree();
        tree.SetRoot(parent);

        ODocument child = tree.AddChild(parent, newName, newIdentifier.toString());
        buildTree(tree, child.getIdentity(), newName, subnodes, depth - 1, startLetter);
        db.commit();
        break;

      } catch (ONeedRetryException e) {
        System.out.println("Concurrency change, retry " + i);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }

    // printPersons("After addSubTree", db);
    db.close();
  }

  public static class AddThread implements Runnable {
    String m_name;
    char   m_startLetter;
    Thread t;

    AddThread(String name, char startLetter) {
      m_name = name;
      m_startLetter = startLetter;
      t = new Thread(this);
      t.start();
    }

    public void run() {
      addSubTree(m_name, m_startLetter);
    }
  }

  public static void deleteSubTree(String parentName) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");
    for (int i = 0;; ++i) {
      try {
        final List<ODocument> result = db.command(new OCommandSQL("select from Person where name = '" + parentName + "'"))
            .execute();
        ODocument parent = result.get(0);
        if (parent == null) {
          db.close();
          return;
        }

        db.begin();

        Collection<ODocument> out = parent.field("out");
        if (out.size() > 0) {
          ODocument edge = out.iterator().next();
          if (edge != null) {
            out.remove(edge);
            final List<ODocument> result2 = db.command(new OCommandSQL("traverse out from " + edge.getIdentity())).execute();
            for (ODocument d : result2) {
              db.delete(d);
            }
          }
        }
        parent.save();
        db.commit();
        break;

      } catch (ONeedRetryException e) {
        System.out.println("Concurrency change, retry " + i);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }

    db.close();
  }

  public static class DeleteThread implements Runnable {
    String m_name;
    Thread t;

    DeleteThread(String name) {
      m_name = name;
      t = new Thread(this);
      t.start();
    }

    public void run() {
      deleteSubTree(m_name);
    }
  }

  public static void main(String[] args) {
    int tries = 20;
    for (int cnt = 0; cnt < tries; cnt++) {

      // SETUP DB
      try {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);

        System.out.println("Recreating database");
        if (ODatabaseHelper.existsDatabase(db, "plocal")) {
          db.setProperty("security", Boolean.FALSE);
          ODatabaseHelper.dropDatabase(db, url, "plocal");
        }
        ODatabaseHelper.createDatabase(db, url);
        db.close();
      } catch (IOException ex) {
        System.out.println("Exception: " + ex);
      }

      // OPEN DB, Create Schema
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");

      OClass vertexClass = db.getMetadata().getSchema().getClass("V");
      OClass edgeClass = db.getMetadata().getSchema().getClass("E");

      OClass personClass = db.getMetadata().getSchema().createClass("Person", vertexClass);
      personClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
      personClass.createProperty("identifier", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      db.getMetadata().getSchema().save();

      // POPULATE TREE
      db.begin();
      PersonTree tree = new PersonTree();
      tree.AddRoot("A", "B");
      buildTree(tree, tree.root.getIdentity(), "A", subnodes, depth, 'A');
      db.commit();

      char startLetter = 'A' + subnodes;
      try {
        AddThread t1 = new AddThread("A", startLetter++);
        DeleteThread t2 = new DeleteThread("A");
        DeleteThread t3 = new DeleteThread("A");
        AddThread t4 = new AddThread("A", startLetter);
        t1.t.join();
        t2.t.join();
        t3.t.join();
        t4.t.join();
      } catch (InterruptedException ex) {
        System.out.println("Interrupted");
      }

      if (!checkIndexConsistency(db))
        cnt = tries;
      db.close();
    }
  }
}
