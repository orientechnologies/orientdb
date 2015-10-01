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
    ODirtyManager real = getReal();
    if (record.getIdentity().isNew()) {
      if (real.newRecord == null)
        real.newRecord = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      real.newRecord.add(record);
    } else {
      if (real.updateRecord == null)
        real.updateRecord = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      real.updateRecord.add(record);
    }
  }

  public ODirtyManager getReal() {
    ODirtyManager real = this;
    while (real.overrider != null) {
      real = real.overrider;
    }
    if (this.overrider != null && this.overrider != real)
      this.overrider = real;
    return real;
  }

  public Set<ORecord> getNewRecord() {
    return getReal().newRecord;
  }

  public Set<ORecord> getUpdateRecord() {
    return getReal().updateRecord;
  }

  public Map<ODocument, List<OIdentifiable>> getReferences() {
    return getReal().references;
  }

  public boolean isSame(ODirtyManager other) {
    // other = other.getReal();
    // if (overrider != null)
    // return overrider.isSame(other);
    return this.getReal() == other.getReal();
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
    getReal().internalTrack(pointing, pointed);
  }

  public void unTrack(ORecord pointing, OIdentifiable pointed) {
    getReal().internalUnTrack(pointing, pointed);
  }

  private void internalUnTrack(ORecord pointing, OIdentifiable pointed) {
    if (references == null)
      return;

    if (pointed.getIdentity().isNew()) {
      List<OIdentifiable> refs = references.get(pointing);
      if (refs == null)
        return;
      if (!(pointed instanceof ODocument) || !((ODocument) pointed).isEmbedded()) {
        refs.remove(pointed);
      }
    }
  }

  private void internalTrack(ORecord pointing, OIdentifiable pointed) {
    if (pointing instanceof ODocument) {
      if (((ODocument) pointing).isEmbedded()) {

        ORecordElement ele = pointing.getOwner();
        while (!(ele instanceof ODocument) && ele != null && ele.getOwner() != null)
          ele = ele.getOwner();
        if (ele != null)
          pointing = (ORecord) ele;
      }
    }
    if (pointed.getIdentity().isNew()) {
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
      ORecordInternal.setDirtyManager((ORecord) pointed, this);
    }
  }

  private void override(ODirtyManager oDirtyManager) {
    ODirtyManager real = getReal();
    oDirtyManager = oDirtyManager.getReal();
    if (real == oDirtyManager)
      return;
    real.overrider = oDirtyManager;
    real.newRecord = null;
    real.updateRecord = null;
    real.references = null;
  }

  public void cleanForSave() {
    ODirtyManager real = getReal();
    real.newRecord = null;
    real.updateRecord = null;
  }

  public List<OIdentifiable> getPointed(ORecord rec) {
    ODirtyManager real = getReal();
    if (real.references == null)
      return null;
    return real.references.get(rec);
  }

  public void removeNew(ORecord record) {
    ODirtyManager real = getReal();
    if (real.newRecord != null)
      real.newRecord.remove(record);
  }

  public void removePointed(ORecord record) {
    ODirtyManager real = getReal();
    if (real.references != null) {
      real.references.remove(record);
      if (real.references.size() == 0)
        references = null;
    }
  }

}
