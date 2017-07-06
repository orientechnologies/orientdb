package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.id.ORID;
import org.mockito.Mockito;
import org.junit.Assert; import org.junit.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;

public class TestImportRewriteLinks {

  @Test
  public void testNestedLinkRewrite() {
    // Fx for remove dirty database in the thread local
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    OIndex<OIdentifiable> mapper = Mockito.mock(OIndex.class);

    Mockito.when(mapper.get(new ORecordId(10, 4))).thenReturn(new ORecordId(10, 3));
    Mockito.when(mapper.get(new ORecordId(11, 1))).thenReturn(new ORecordId(21, 1));
    Mockito.when(mapper.get(new ORecordId(31, 1))).thenReturn(new ORecordId(41, 1));
    Mockito.when(mapper.get(new ORecordId(51, 1))).thenReturn(new ORecordId(61, 1));

    final Set<ORID> brokenRids = new HashSet<ORID>();

    ODocument doc = new ODocument();
    ODocument emb = new ODocument();
    doc.field("emb", emb, OType.EMBEDDED);
    ODocument emb1 = new ODocument();
    emb.field("emb1", emb1, OType.EMBEDDED);
    emb1.field("link", new ORecordId(10, 4));
    emb1.field("brokenLink", new ORecordId(10, 5));
    emb1.field("negativeLink", new ORecordId(-1, -42));

    List<OIdentifiable> linkList = new ArrayList<OIdentifiable>();

    linkList.add(new ORecordId(-1, -42));
    linkList.add(new ORecordId(11, 2));
    linkList.add(new ORecordId(11, 1));

    brokenRids.add(new ORecordId(10, 5));
    brokenRids.add(new ORecordId(11, 2));
    brokenRids.add(new ORecordId(31, 2));
    brokenRids.add(new ORecordId(51, 2));

    Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();

    linkSet.add(new ORecordId(-1, -42));
    linkSet.add(new ORecordId(31, 2));
    linkSet.add(new ORecordId(31, 1));

    Map<String, OIdentifiable> linkMap = new HashMap<String, OIdentifiable>();

    linkMap.put("key1", new ORecordId(51, 1));
    linkMap.put("key2", new ORecordId(51, 2));
    linkMap.put("key3", new ORecordId(-1, -42));

    emb1.field("linkList", linkList);
    emb1.field("linkSet", linkSet);
    emb1.field("linkMap", linkMap);

    ODatabaseImport.rewriteLinksInDocument(doc, mapper, brokenRids);

    Assert.assertEquals(emb1.field("link"), new ORecordId(10, 3));
    Assert.assertEquals(emb1.field("negativeLink"), new ORecordId(-1, -42));
    Assert.assertEquals(emb1.field("brokenLink"), null);

    List<OIdentifiable> resLinkList = new ArrayList<OIdentifiable>();
    resLinkList.add(new ORecordId(-1, -42));
    resLinkList.add(new ORecordId(21, 1));

    Assert.assertEquals(emb1.field("linkList"), resLinkList);

    Set<OIdentifiable> resLinkSet = new HashSet<OIdentifiable>();
    resLinkSet.add(new ORecordId(41, 1));
    resLinkSet.add(new ORecordId(-1, -42));

    Assert.assertEquals(emb1.field("linkSet"), resLinkSet);

    Map<String, OIdentifiable> resLinkMap = new HashMap<String, OIdentifiable>();
    resLinkMap.put("key1", new ORecordId(61, 1));
    resLinkMap.put("key3", new ORecordId(-1, -42));

    Assert.assertEquals(emb1.field("linkMap"), resLinkMap);
  }

}
