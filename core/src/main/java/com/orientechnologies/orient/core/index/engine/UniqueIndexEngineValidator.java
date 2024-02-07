package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

public class UniqueIndexEngineValidator implements IndexEngineValidator<Object, ORID> {

  /** */
  private final OIndexUnique indexUnique;

  /**
   * @param oIndexUnique
   */
  public UniqueIndexEngineValidator(OIndexUnique oIndexUnique) {
    indexUnique = oIndexUnique;
  }

  @Override
  public Object validate(Object key, ORID oldValue, ORID newValue) {
    if (oldValue != null) {
      ODocument metadata = indexUnique.getMetadata();
      // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
      if (!oldValue.equals(newValue)) {
        final Boolean mergeSameKey =
            metadata != null ? (Boolean) metadata.field(OIndexInternal.MERGE_KEYS) : Boolean.FALSE;
        if (mergeSameKey == null || !mergeSameKey) {
          throw new ORecordDuplicatedException(
              String.format(
                  "Cannot index record %s: found duplicated key '%s' in index '%s' previously"
                      + " assigned to the record %s",
                  newValue.getIdentity(), key, indexUnique.getName(), oldValue.getIdentity()),
              indexUnique.getName(),
              oldValue.getIdentity(),
              key);
        }
      } else {
        return IndexEngineValidator.IGNORE;
      }
    }

    if (!newValue.getIdentity().isPersistent()) {
      newValue = newValue.getRecord().getIdentity();
    }
    return newValue.getIdentity();
  }
}
