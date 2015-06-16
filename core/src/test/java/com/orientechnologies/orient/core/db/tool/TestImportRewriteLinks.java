package com.orientechnologies.orient.core.db.tool;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class TestImportRewriteLinks {

  @Test
  public void testNestedLinkRewrite() {
    OIndex<OIdentifiable> mapper = Mockito.mock(OIndex.class);
    Mockito.when(mapper.get(new ORecordId(10, 4))).thenReturn(new ORecordId(10, 3));

    ODocument doc = new ODocument();
    ODocument emb = new ODocument();
    doc.field("emb", emb, OType.EMBEDDED);
    ODocument emb1 = new ODocument();
    emb.field("emb1", emb1, OType.EMBEDDED);
    emb1.field("link", new ORecordId(10, 4));

    ODatabaseImport.rewriteLinksInDocument(doc, mapper);
    Assert.assertEquals(emb1.field("link"), new ORecordId(10, 3));

  }

}
