package com.orientechnologies.orient.core.index.multivalue;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class OMultivalueEntityRemover implements OIndexKeyUpdater<Object> {
  private final OIdentifiable value;
  private final OModifiableBoolean removed;

  public OMultivalueEntityRemover(OIdentifiable value, OModifiableBoolean removed) {
    this.value = value;
    this.removed = removed;
  }

  @Override
  public OIndexUpdateAction<Object> update(Object persistentValue, AtomicLong bonsayFileId) {
    @SuppressWarnings("unchecked")
    Set<OIdentifiable> values = (Set<OIdentifiable>) persistentValue;
    if (value == null) {
      removed.setValue(true);

      //noinspection unchecked
      return OIndexUpdateAction.remove();
    } else if (values.remove(value)) {
      removed.setValue(true);

      if (values.isEmpty()) {
        // remove tree ridbag too
        if (values instanceof OMixedIndexRIDContainer) {
          ((OMixedIndexRIDContainer) values).delete();
        } else if (values instanceof OIndexRIDContainerSBTree) {
          ((OIndexRIDContainerSBTree) values).delete();
        }

        //noinspection unchecked
        return OIndexUpdateAction.remove();
      } else {
        return OIndexUpdateAction.changed(values);
      }
    }

    return OIndexUpdateAction.changed(values);
  }
}
