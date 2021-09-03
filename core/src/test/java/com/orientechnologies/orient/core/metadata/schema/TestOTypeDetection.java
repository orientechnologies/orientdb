package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class TestOTypeDetection {

  @Test
  public void testOTypeFromClass() {

    assertEquals(OType.BOOLEAN, OType.getTypeByClass(Boolean.class));

    assertEquals(OType.BOOLEAN, OType.getTypeByClass(Boolean.TYPE));

    assertEquals(OType.LONG, OType.getTypeByClass(Long.class));

    assertEquals(OType.LONG, OType.getTypeByClass(Long.TYPE));

    assertEquals(OType.INTEGER, OType.getTypeByClass(Integer.class));

    assertEquals(OType.INTEGER, OType.getTypeByClass(Integer.TYPE));

    assertEquals(OType.SHORT, OType.getTypeByClass(Short.class));

    assertEquals(OType.SHORT, OType.getTypeByClass(Short.TYPE));

    assertEquals(OType.FLOAT, OType.getTypeByClass(Float.class));

    assertEquals(OType.FLOAT, OType.getTypeByClass(Float.TYPE));

    assertEquals(OType.DOUBLE, OType.getTypeByClass(Double.class));

    assertEquals(OType.DOUBLE, OType.getTypeByClass(Double.TYPE));

    assertEquals(OType.BYTE, OType.getTypeByClass(Byte.class));

    assertEquals(OType.BYTE, OType.getTypeByClass(Byte.TYPE));

    assertEquals(OType.STRING, OType.getTypeByClass(Character.class));

    assertEquals(OType.STRING, OType.getTypeByClass(Character.TYPE));

    assertEquals(OType.STRING, OType.getTypeByClass(String.class));

    // assertEquals(OType.BINARY, OType.getTypeByClass(Byte[].class));

    assertEquals(OType.BINARY, OType.getTypeByClass(byte[].class));

    assertEquals(OType.DATETIME, OType.getTypeByClass(Date.class));

    assertEquals(OType.DECIMAL, OType.getTypeByClass(BigDecimal.class));

    assertEquals(OType.INTEGER, OType.getTypeByClass(BigInteger.class));

    assertEquals(OType.LINK, OType.getTypeByClass(OIdentifiable.class));

    assertEquals(OType.LINK, OType.getTypeByClass(ORecordId.class));

    assertEquals(OType.LINK, OType.getTypeByClass(ORecord.class));

    assertEquals(OType.LINK, OType.getTypeByClass(ODocument.class));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByClass(ArrayList.class));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByClass(List.class));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByClass(OTrackedList.class));

    assertEquals(OType.EMBEDDEDSET, OType.getTypeByClass(Set.class));

    assertEquals(OType.EMBEDDEDSET, OType.getTypeByClass(HashSet.class));

    assertEquals(OType.EMBEDDEDSET, OType.getTypeByClass(OTrackedSet.class));

    assertEquals(OType.EMBEDDEDMAP, OType.getTypeByClass(Map.class));

    assertEquals(OType.EMBEDDEDMAP, OType.getTypeByClass(HashMap.class));

    assertEquals(OType.EMBEDDEDMAP, OType.getTypeByClass(OTrackedMap.class));

    assertEquals(OType.LINKSET, OType.getTypeByClass(ORecordLazySet.class));

    assertEquals(OType.LINKLIST, OType.getTypeByClass(ORecordLazyList.class));

    assertEquals(OType.LINKMAP, OType.getTypeByClass(ORecordLazyMap.class));

    assertEquals(OType.LINKBAG, OType.getTypeByClass(ORidBag.class));

    assertEquals(OType.CUSTOM, OType.getTypeByClass(OSerializableStream.class));

    assertEquals(OType.CUSTOM, OType.getTypeByClass(CustomClass.class));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByClass(Object[].class));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByClass(String[].class));

    assertEquals(OType.EMBEDDED, OType.getTypeByClass(ODocumentSerializable.class));

    assertEquals(OType.EMBEDDED, OType.getTypeByClass(DocumentSer.class));

    assertEquals(OType.CUSTOM, OType.getTypeByClass(ClassSerializable.class));
  }

  @Test
  public void testOTypeFromValue() {

    assertEquals(OType.BOOLEAN, OType.getTypeByValue(true));

    assertEquals(OType.LONG, OType.getTypeByValue(2L));

    assertEquals(OType.INTEGER, OType.getTypeByValue(2));

    assertEquals(OType.SHORT, OType.getTypeByValue((short) 4));

    assertEquals(OType.FLOAT, OType.getTypeByValue(0.5f));

    assertEquals(OType.DOUBLE, OType.getTypeByValue(0.7d));

    assertEquals(OType.BYTE, OType.getTypeByValue((byte) 10));

    assertEquals(OType.STRING, OType.getTypeByValue('a'));

    assertEquals(OType.STRING, OType.getTypeByValue("yaaahooooo"));

    assertEquals(OType.BINARY, OType.getTypeByValue(new byte[] {0, 1, 2}));

    assertEquals(OType.DATETIME, OType.getTypeByValue(new Date()));

    assertEquals(OType.DECIMAL, OType.getTypeByValue(new BigDecimal(10)));

    assertEquals(OType.INTEGER, OType.getTypeByValue(new BigInteger("20")));

    assertEquals(OType.LINK, OType.getTypeByValue(new ODocument()));

    assertEquals(OType.LINK, OType.getTypeByValue(new ORecordId()));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByValue(new ArrayList<Object>()));

    assertEquals(
        OType.EMBEDDEDLIST, OType.getTypeByValue(new OTrackedList<Object>(new ODocument())));

    assertEquals(OType.EMBEDDEDSET, OType.getTypeByValue(new HashSet<Object>()));

    assertEquals(OType.EMBEDDEDMAP, OType.getTypeByValue(new HashMap<Object, Object>()));

    assertEquals(OType.LINKSET, OType.getTypeByValue(new ORecordLazySet(new ODocument())));

    assertEquals(OType.LINKLIST, OType.getTypeByValue(new ORecordLazyList(new ODocument())));

    assertEquals(OType.LINKMAP, OType.getTypeByValue(new ORecordLazyMap(new ODocument())));

    assertEquals(OType.LINKBAG, OType.getTypeByValue(new ORidBag()));

    assertEquals(OType.CUSTOM, OType.getTypeByValue(new CustomClass()));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByValue(new Object[] {}));

    assertEquals(OType.EMBEDDEDLIST, OType.getTypeByValue(new String[] {}));

    assertEquals(OType.EMBEDDED, OType.getTypeByValue(new DocumentSer()));

    assertEquals(OType.CUSTOM, OType.getTypeByValue(new ClassSerializable()));
  }

  @Test
  public void testOTypeFromValueInternal() {
    Map<String, ORecordId> linkmap = new HashMap<String, ORecordId>();
    linkmap.put("some", new ORecordId());
    assertEquals(OType.LINKMAP, OType.getTypeByValue(linkmap));

    Map<String, ORecord> linkmap2 = new HashMap<String, ORecord>();
    linkmap2.put("some", new ODocument());
    assertEquals(OType.LINKMAP, OType.getTypeByValue(linkmap2));

    List<ORecordId> linkList = new ArrayList<ORecordId>();
    linkList.add(new ORecordId());
    assertEquals(OType.LINKLIST, OType.getTypeByValue(linkList));

    List<ORecord> linkList2 = new ArrayList<ORecord>();
    linkList2.add(new ODocument());
    assertEquals(OType.LINKLIST, OType.getTypeByValue(linkList2));

    Set<ORecordId> linkSet = new HashSet<ORecordId>();
    linkSet.add(new ORecordId());
    assertEquals(OType.LINKSET, OType.getTypeByValue(linkSet));

    Set<ORecord> linkSet2 = new HashSet<ORecord>();
    linkSet2.add(new ODocument());
    assertEquals(OType.LINKSET, OType.getTypeByValue(linkSet2));

    ODocument document = new ODocument();
    ODocumentInternal.addOwner(document, new ODocument());
    assertEquals(OType.EMBEDDED, OType.getTypeByValue(document));
  }

  public class CustomClass implements OSerializableStream {

    @Override
    public byte[] toStream() throws OSerializationException {
      return null;
    }

    @Override
    public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
      return null;
    }
  }

  public class DocumentSer implements ODocumentSerializable {

    @Override
    public ODocument toDocument() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void fromDocument(ODocument document) {
      // TODO Auto-generated method stub

    }
  }

  public class ClassSerializable implements Serializable {
    private String aaa;
  }
}
