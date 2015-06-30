package com.orientechnologies.orient.core.record.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class ODirtyManager {

  private ODirtyManager                       overrider;
  private Map<ODocument, List<OIdentifiable>> references;
  private Set<ORecord>                        newRecord;
  private Set<ORecord>                        updateRecord;

  public void setDirty(ORecord record) {
    if (overrider != null)
      overrider.setDirty(record);
    else {
      if (record.getIdentity().isNew()) {
        if (newRecord == null)
          newRecord = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
        newRecord.add(record);
      } else {
        if (updateRecord == null)
          updateRecord = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
        updateRecord.add(record);
      }
    }
  }

  public Set<ORecord> getNewRecord() {
    if (overrider != null)
      return overrider.getNewRecord();
    return newRecord;
  }

  public Set<ORecord> getUpdateRecord() {
    if (overrider != null)
      return overrider.getUpdateRecord();
    return updateRecord;
  }

  public Map<ODocument, List<OIdentifiable>> getReferences() {
    if (overrider != null)
      return overrider.getReferences();
    return references;
  }

  public boolean isSame(ODirtyManager other) {
    if (overrider != null)
      return overrider.isSame(other);
    return this == other;
  }

  public void merge(ODirtyManager toMerge) {
    if (isSame(toMerge))
      return;
    if (toMerge.getNewRecord() != null) {
      if (newRecord == null)
        newRecord = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      this.newRecord.addAll(toMerge.getNewRecord());
    }
    if (toMerge.getUpdateRecord() != null) {
      if (updateRecord == null)
        updateRecord = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      this.updateRecord.addAll(toMerge.getUpdateRecord());
    }
    if (toMerge.getReferences() != null) {
      if (references == null)
        references = new IdentityHashMap<ODocument, List<OIdentifiable>>();
      for (Entry<ODocument, List<OIdentifiable>> entry : toMerge.getReferences().entrySet()) {
        List<OIdentifiable> refs = references.get(entry.getKey());
        if (refs == null)
          references.put(entry.getKey(), entry.getValue());
        else
          refs.addAll(entry.getValue());
      }
    }
    toMerge.override(this);
  }

  public void track(ORecord pointing, OIdentifiable pointed) {
    if (overrider != null) {
      overrider.track(pointing, pointed);
      return;
    }
    if (pointing instanceof ODocument) {
      if (((ODocument) pointing).isEmbedded()) {

        ORecordElement ele = pointing.getOwner();
        while (!(ele instanceof ODocument) && ele != null && ((ODocument) pointing).isEmbedded())
          ele = ele.getOwner();
        pointing = (ORecord) ele;
      }
    }
    if (pointed.getIdentity().isNew() && pointing.getIdentity().isNew()) {
      if (!(pointed instanceof ODocument) || !((ODocument) pointed).isEmbedded()) {
        if (references == null) {
          references = new IdentityHashMap<ODocument, List<OIdentifiable>>();
        }
        List<OIdentifiable> refs = references.get(pointing);
        if (refs == null) {
          refs = new ArrayList<OIdentifiable>();
          references.put((ODocument) pointing, refs);
        }
        refs.add(pointed);
      } else if (pointed instanceof ODocument) {
        List<OIdentifiable> point = ORecordInternal.getDirtyManager((ORecord) pointed).getPointed((ORecord) pointed);
        if (point != null && point.size() > 0) {
          if (references == null) {
            references = new IdentityHashMap<ODocument, List<OIdentifiable>>();
          }
          List<OIdentifiable> refs = references.get(pointing);
          if (refs == null) {
            refs = new ArrayList<OIdentifiable>();
            references.put((ODocument) pointing, refs);
          }
          for (OIdentifiable embPoint : point) {
            refs.add(embPoint);
          }
        }
      }
    }
    if (pointed instanceof ORecord) {
      ORecordInternal.setDirtyManager((ORecord) pointed, ORecordInternal.getDirtyManager(pointing));
    }

  }

  private void override(ODirtyManager oDirtyManager) {
    if (this == oDirtyManager)
      return;
    if (this.overrider != null)
      this.overrider.override(oDirtyManager);
    else {
      this.overrider = oDirtyManager;
      this.newRecord = null;
      this.updateRecord = null;
      this.references = null;
    }
  }

  public void cleanForSave() {
    this.newRecord = null;
    this.updateRecord = null;
  }

  public List<OIdentifiable> getPointed(ORecord rec) {
    if (overrider != null)
      return overrider.getPointed(rec);

    if (references == null)
      return null;
    return references.get(rec);
  }

  public void removeNew(ODocument oDocument) {
    if (overrider != null)
      this.overrider.removeNew(oDocument);
    if (this.newRecord != null)
      this.newRecord.remove(oDocument);
  }
}
