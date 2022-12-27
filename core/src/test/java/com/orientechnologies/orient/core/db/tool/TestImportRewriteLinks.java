package com.orientechnologies.orient.core.db.tool;

import static com.orientechnologies.orient.core.db.tool.ODatabaseImport.EXPORT_IMPORT_CLASS_NAME;
import static com.orientechnologies.orient.core.db.tool.ODatabaseImport.EXPORT_IMPORT_INDEX_NAME;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TestImportRewriteLinks {

  @Test
  public void testNestedLinkRewrite() {
    try (final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "testDB", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      try (final ODatabaseSession session =
          orientDb.open("testDB", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        final OSchema schema = session.getMetadata().getSchema();

        final OClass cls = schema.createClass(EXPORT_IMPORT_CLASS_NAME);
        cls.createProperty("key", OType.STRING);
        cls.createProperty("value", OType.STRING);
        cls.createIndex(EXPORT_IMPORT_INDEX_NAME, OClass.INDEX_TYPE.DICTIONARY, "key");

        new ODocument(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new ORecordId(10, 4).toString())
            .field("value", new ORecordId(10, 3).toString())
            .save();

        new ODocument(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new ORecordId(11, 1).toString())
            .field("value", new ORecordId(21, 1).toString())
            .save();

        new ODocument(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new ORecordId(31, 1).toString())
            .field("value", new ORecordId(41, 1).toString())
            .save();

        new ODocument(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new ORecordId(51, 1).toString())
            .field("value", new ORecordId(61, 1).toString())
            .save();

        final Set<ORID> brokenRids = new HashSet<>();

        ODocument doc = new ODocument();

        ODocument emb = new ODocument();
        doc.field("emb", emb, OType.EMBEDDED);
        ODocument emb1 = new ODocument();
        emb.field("emb1", emb1, OType.EMBEDDED);
        emb1.field("link", new ORecordId(10, 4));
        emb1.field("brokenLink", new ORecordId(10, 5));
        emb1.field("negativeLink", new ORecordId(-1, -42));

        List<OIdentifiable> linkList = new ArrayList<>();

        linkList.add(new ORecordId(-1, -42));
        linkList.add(new ORecordId(11, 2));
        linkList.add(new ORecordId(11, 1));

        brokenRids.add(new ORecordId(10, 5));
        brokenRids.add(new ORecordId(11, 2));
        brokenRids.add(new ORecordId(31, 2));
        brokenRids.add(new ORecordId(51, 2));

        Set<OIdentifiable> linkSet = new HashSet<>();

        linkSet.add(new ORecordId(-1, -42));
        linkSet.add(new ORecordId(31, 2));
        linkSet.add(new ORecordId(31, 1));

        Map<String, OIdentifiable> linkMap = new HashMap<>();

        linkMap.put("key1", new ORecordId(51, 1));
        linkMap.put("key2", new ORecordId(51, 2));
        linkMap.put("key3", new ORecordId(-1, -42));

        emb1.field("linkList", linkList);
        emb1.field("linkSet", linkSet);
        emb1.field("linkMap", linkMap);

        ODatabaseImport.doRewriteLinksInDocument(session, doc, brokenRids);

        Assert.assertEquals(emb1.field("link"), new ORecordId(10, 3));
        Assert.assertEquals(emb1.field("negativeLink"), new ORecordId(-1, -42));
        Assert.assertNull(emb1.field("brokenLink"));

        List<OIdentifiable> resLinkList = new ArrayList<>();
        resLinkList.add(new ORecordId(-1, -42));
        resLinkList.add(new ORecordId(21, 1));

        Assert.assertEquals(emb1.field("linkList"), resLinkList);

        Set<OIdentifiable> resLinkSet = new HashSet<>();
        resLinkSet.add(new ORecordId(41, 1));
        resLinkSet.add(new ORecordId(-1, -42));

        Assert.assertEquals(emb1.field("linkSet"), resLinkSet);

        Map<String, OIdentifiable> resLinkMap = new HashMap<>();
        resLinkMap.put("key1", new ORecordId(61, 1));
        resLinkMap.put("key3", new ORecordId(-1, -42));

        Assert.assertEquals(emb1.field("linkMap"), resLinkMap);
      }
    }
  }
}
