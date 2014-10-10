package com.orientechnologies.orient.core.record.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.record.ORecord;

public class DirtyFinderTest {

  @Test
  public void testNestedDocument() {

    ODocument document = new ODocument();
    document.field("link", new ODocument());
    document.field("linkBytes", new ORecordBytes());

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

  @Test
  public void testNestedListDocument() {
    ODocument document = new ODocument();
    List<ODocument> refereed = new ArrayList<ODocument>();
    refereed.add(new ODocument());
    refereed.add(new ODocument());
    document.field("links", refereed);

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

  @Test
  public void testNestedLazyListDocument() {
    ODocument document = new ODocument();
    List<OIdentifiable> refereed = new ORecordLazyList(document);
    refereed.add(new ODocument());
    refereed.add(new ODocument());
    document.field("links", refereed);

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

  @Test
  public void testNestedSetDocument() {
    ODocument document = new ODocument();
    Set<ODocument> refereed = new HashSet<ODocument>();
    refereed.add(new ODocument());
    refereed.add(new ODocument());
    document.field("links", refereed);

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

  @Test
  public void testNestedLazySetDocument() {
    ODocument document = new ODocument();
    Set<OIdentifiable> refereed = new ORecordLazySet(document);
    refereed.add(new ODocument());
    refereed.add(new ODocument());
    document.field("links", refereed);

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

  @Test
  public void testNestedMapDocument() {
    ODocument document = new ODocument();
    Map<String, ORecord> refereed = new HashMap<String, ORecord>();
    refereed.put("a", new ODocument());
    refereed.put("b", new ODocument());
    document.field("links", refereed);

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

  @Test
  public void testNestedLazyMapDocument() {
    ODocument document = new ODocument();
    Map<Object, OIdentifiable> refereed = new ORecordLazyMap(document);
    refereed.put("a", new ODocument());
    refereed.put("b", new ODocument());
    document.field("links", refereed);

    Set<ORecord> dirties = new HashSet<ORecord>();
    DirtyFinder.findDirties(document, dirties);
    assertEquals(3, dirties.size());
  }

}
