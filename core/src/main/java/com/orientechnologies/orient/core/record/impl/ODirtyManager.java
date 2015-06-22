package com.orientechnologies.orient.core.record.impl;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.record.ORecord;

public class ODirtyManager {

  private ODirtyManager overriden;
  private Set<ORecord>  newRecord;
  private Set<ORecord>  updateRecord;

  public void setDirty(ORecord record) {
    if (overriden != null)
      overriden.setDirty(record);
    else {
      if (record.getIdentity().isNew()) {
        if (newRecord == null)
          newRecord = new HashSet<ORecord>();
        newRecord.add(record);
      } else {
        if (updateRecord == null)
          updateRecord = new HashSet<ORecord>();
        updateRecord.add(record);
      }
    }
  }

  public Set<ORecord> getNewRecord() {
    if (overriden != null)
      return overriden.getNewRecord();
    return newRecord;
  }

  public Set<ORecord> getUpdateRecord() {
    if (overriden != null)
      return overriden.getUpdateRecord();
    return updateRecord;
  }

  public void merge(ODirtyManager toMerge) {
    if (toMerge.getNewRecord() != null) {
      if (newRecord == null)
        newRecord = new HashSet<ORecord>();
      this.newRecord.addAll(toMerge.getNewRecord());
    }
    if (toMerge.getUpdateRecord() != null) {
      if (updateRecord == null)
        updateRecord = new HashSet<ORecord>();
      this.updateRecord.addAll(toMerge.getUpdateRecord());
    }
    toMerge.override(this);
  }

  private void override(ODirtyManager oDirtyManager) {
    if (this.overriden != null)
      this.overriden.override(oDirtyManager);
    else
      this.overriden = oDirtyManager;
  }

  public void cleanForSave() {
    this.newRecord = null;
    this.updateRecord = null;
  }
}
