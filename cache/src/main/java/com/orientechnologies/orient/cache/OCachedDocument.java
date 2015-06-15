package com.orientechnologies.orient.cache;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Represents a minimal ODocument to save previous RAM when it's stored in cache.
 * 
 * @author Luca Garulli (l.garulli-at-orientdb.com)
 */
public class OCachedDocument extends OCachedRecord<ODocument> {
  private String[] fieldNames;
  private Object[] fieldValues;

  public OCachedDocument() {
  }

  public OCachedDocument(final ODocument iDocument) {
    rid = iDocument.getIdentity();
    fieldNames = iDocument.fieldNames();
    fieldValues = new Object[fieldNames.length];
    for (int i = 0; i < fieldNames.length; ++i) {
      fieldValues[i] = convertValueToCached(iDocument.rawField(fieldNames[i]));
    }
  }

  public ODocument toRecord() {
    final ODocument doc = new ODocument(rid);
    doc.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {
      for (int i = 0; i < fieldNames.length; ++i)
        if (fieldNames[i] != null) {
          doc.field(fieldNames[i], convertValueFromCached(doc, fieldValues[i]));
        }
      ORecordInternal.unsetDirty(doc);
    } finally {
      doc.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
    return doc;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeShort(rid.getClusterId());
    out.writeLong(rid.getClusterPosition());
    out.writeInt(fieldNames.length);
    for (int i = 0; i < fieldNames.length; ++i) {
      out.writeUTF(fieldNames[i]);
      out.writeObject(fieldValues[i]);
    }
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rid = new ORecordId(in.readShort(), in.readLong());
    final int fields = in.readInt();
    fieldNames = new String[fields];
    fieldValues = new Object[fields];
    for (int i = 0; i < fields; ++i) {
      fieldNames[i] = in.readUTF();
      fieldValues[i] = in.readObject();
    }
  }

  protected Object convertValueToCached(Object fieldValue) {
    if (fieldValue == null)
      return null;

    if (fieldValue instanceof ORecord && ((ORecord) fieldValue).getIdentity().isPersistent())
      // STORE THE RID ONLY
      fieldValue = new OCachedRID(((ORecord) fieldValue).getIdentity());
    else if (fieldValue instanceof ODocument)
      fieldValue = new OCachedDocument((ODocument) fieldValue);
    else if (fieldValue instanceof ORecordBytes)
      fieldValue = new OCachedRecordBytes((ORecordBytes) fieldValue);
    else if (fieldValue instanceof ORecordLazySet)
      fieldValue = convertCollectionToCached(((ORecordLazySet) fieldValue).rawIterator(), new HashSet());
    else if (fieldValue instanceof OTrackedSet)
      fieldValue = convertCollectionToCached(((OTrackedSet) fieldValue).iterator(), new HashSet());
    else if (fieldValue instanceof ORecordLazyList)
      fieldValue = convertCollectionToCached(((ORecordLazyList) fieldValue).rawIterator(), new ArrayList<OIdentifiable>());
    else if (fieldValue instanceof OTrackedList)
      fieldValue = convertCollectionToCached(((OTrackedList) fieldValue).iterator(), new ArrayList<OIdentifiable>());
    else if (fieldValue instanceof Collection)
      fieldValue = convertCollectionToCached(((Collection) fieldValue).iterator(), new ArrayList<OIdentifiable>());
    else if (fieldValue instanceof ORidBag)
      fieldValue = convertCollectionToCached(((ORidBag) fieldValue).rawIterator(), new HashSet<OIdentifiable>());

    return fieldValue;
  }

  protected Collection convertCollectionToCached(final Iterator<OIdentifiable> it, final Collection target) {
    while (it.hasNext())
      target.add(convertValueToCached(it.next()));
    return target;
  }

  protected Object convertValueFromCached(final ODocument doc, Object fieldValue) {
    if (fieldValue == null)
      return null;

    if (fieldValue instanceof OCachedRecord)
      fieldValue = ((OCachedRecord) fieldValue).toRecord();
    else if (fieldValue instanceof OCachedRID)
      fieldValue = ((OCachedRID) fieldValue).toRID();
    else if (fieldValue instanceof Set)
      fieldValue = convertCollectionFromCached(doc, ((Set) fieldValue).iterator(), new HashSet());
    else if (fieldValue instanceof List)
      fieldValue = convertCollectionFromCached(doc, ((List) fieldValue).iterator(), new ArrayList<OIdentifiable>());
    else if (fieldValue instanceof Collection)
      fieldValue = convertCollectionFromCached(doc, ((Collection) fieldValue).iterator(), new ArrayList<OIdentifiable>());

    if (fieldValue instanceof ODocument)
      ODocumentInternal.addOwner((ODocument) fieldValue, doc);

    return fieldValue;
  }

  protected Collection convertCollectionFromCached(final ODocument doc, final Iterator<OIdentifiable> it, final Collection target) {
    while (it.hasNext())
      target.add(convertValueFromCached(doc, it.next()));
    return target;
  }
}
