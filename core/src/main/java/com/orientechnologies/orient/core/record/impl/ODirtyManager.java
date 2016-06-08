/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author Emanuele Tagliafetti
 */
public class ODirtyManager {

  private ODirtyManager                       overrider;
  private Map<ODocument, List<OIdentifiable>> references;
  private Set<ORecord>                        newRecords;
  private Set<ORecord>                        updateRecords;

  public void setDirty(ORecord record) {
    ODirtyManager real = getReal();
    if (record.getIdentity().isNew() && !record.getIdentity().isTemporary()) {
      if (real.newRecords == null)
        real.newRecords = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      real.newRecords.add(record);
    } else {
      if (real.updateRecords == null)
        real.updateRecords = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      real.updateRecords.add(record);
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

  public Set<ORecord> getNewRecords() {
    return getReal().newRecords;
  }

  public Set<ORecord> getUpdateRecords() {
    return getReal().updateRecords;
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
    if (toMerge.getNewRecords() != null) {
      if (newRecords == null)
        newRecords = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      this.newRecords.addAll(toMerge.getNewRecords());
    }
    if (toMerge.getUpdateRecords() != null) {
      if (updateRecords == null)
        updateRecords = Collections.newSetFromMap(new IdentityHashMap<ORecord, Boolean>());
      this.updateRecords.addAll(toMerge.getUpdateRecords());
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
    real.newRecords = null;
    real.updateRecords = null;
    real.references = null;
  }

  public void clearForSave() {
    ODirtyManager real = getReal();
    real.newRecords = null;
    real.updateRecords = null;
  }

  public List<OIdentifiable> getPointed(ORecord rec) {
    ODirtyManager real = getReal();
    if (real.references == null)
      return null;
    return real.references.get(rec);
  }

  public void removeNew(ORecord record) {
    ODirtyManager real = getReal();
    if (real.newRecords != null)
      real.newRecords.remove(record);
  }

  public void removePointed(ORecord record) {
    ODirtyManager real = getReal();
    if (real.references != null) {
      real.references.remove(record);
      if (real.references.size() == 0)
        references = null;
    }
  }

  public void clear() {
    clearForSave();
    getReal().references = null;
  }
}
