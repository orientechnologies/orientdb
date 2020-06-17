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
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** @author Emanuele Tagliafetti */
public class ODirtyManager {

  private ODirtyManager overrider;
  private Set<ORecord> newRecords;
  private Set<ORecord> updateRecords;

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
    if (this.overrider != null && this.overrider != real) this.overrider = real;
    return real;
  }

  public Set<ORecord> getNewRecords() {
    return getReal().newRecords;
  }

  public Set<ORecord> getUpdateRecords() {
    return getReal().updateRecords;
  }

  public boolean isSame(ODirtyManager other) {
    return this.getReal() == other.getReal();
  }

  public void merge(ODirtyManager toMerge) {
    if (isSame(toMerge)) return;
    this.newRecords = mergeSet(this.newRecords, toMerge.getNewRecords());
    this.updateRecords = mergeSet(this.updateRecords, toMerge.getUpdateRecords());
    toMerge.override(this);
  }

  /**
   * Merge the two set try to use the optimum case
   *
   * @param target
   * @param source
   * @return
   */
  private static Set<ORecord> mergeSet(Set<ORecord> target, Set<ORecord> source) {
    if (source != null) {
      if (target == null) {
        return source;
      } else {
        if (target.size() > source.size()) {
          target.addAll(source);
          return target;
        } else {
          source.addAll(target);
          return source;
        }
      }
    } else {
      return target;
    }
  }

  public void track(ORecord pointing, OIdentifiable pointed) {
    getReal().internalTrack(pointing, pointed);
  }

  public void unTrack(ORecord pointing, OIdentifiable pointed) {
    getReal().internalUnTrack(pointing, pointed);
  }

  private void internalUnTrack(ORecord pointing, OIdentifiable pointed) {}

  private void internalTrack(ORecord pointing, OIdentifiable pointed) {
    if (pointed instanceof ORecord) {
      ORecordInternal.setDirtyManager((ORecord) pointed, this);
    }
  }

  private void override(ODirtyManager oDirtyManager) {
    ODirtyManager real = getReal();
    oDirtyManager = oDirtyManager.getReal();
    if (real == oDirtyManager) return;
    real.overrider = oDirtyManager;
    real.newRecords = null;
    real.updateRecords = null;
  }

  public void clearForSave() {
    ODirtyManager real = getReal();
    real.newRecords = null;
    real.updateRecords = null;
  }

  public void removeNew(ORecord record) {
    ODirtyManager real = getReal();
    if (real.newRecords != null) real.newRecords.remove(record);
  }

  public void clear() {
    clearForSave();
  }
}
