package com.orientechnologies.orient.core.index.multivalue;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.ODefaultIndexFactory;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public final class OMultivalueIndexKeyUpdaterImpl implements OIndexKeyUpdater<Object> {
  private final ORID identity;
  private final String valueContainerAlgorithm;
  private final int binaryFormatVersion;
  private final String indexName;

  public OMultivalueIndexKeyUpdaterImpl(
      ORID identity, String valueContainerAlgorithm, int binaryFormatVersion, String indexName) {
    this.identity = identity;
    this.valueContainerAlgorithm = valueContainerAlgorithm;
    this.binaryFormatVersion = binaryFormatVersion;
    this.indexName = indexName;
  }

  @Override
  public OIndexUpdateAction<Object> update(Object oldValue, AtomicLong bonsayFileId) {
    @SuppressWarnings("unchecked")
    Set<OIdentifiable> toUpdate = (Set<OIdentifiable>) oldValue;
    if (toUpdate == null) {
      if (ODefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
        if (binaryFormatVersion >= 13) {
          toUpdate = new OMixedIndexRIDContainer(indexName, bonsayFileId);
        } else {
          toUpdate = new OIndexRIDContainer(indexName, true, bonsayFileId);
        }
      } else {
        throw new IllegalStateException("MVRBTree is not supported any more");
      }
    }
    if (toUpdate instanceof OIndexRIDContainer) {
      boolean isTree = !((OIndexRIDContainer) toUpdate).isEmbedded();
      toUpdate.add(identity);

      if (isTree) {
        //noinspection unchecked
        return OIndexUpdateAction.nothing();
      } else {
        return OIndexUpdateAction.changed(toUpdate);
      }
    } else if (toUpdate instanceof OMixedIndexRIDContainer) {
      final OMixedIndexRIDContainer ridContainer = (OMixedIndexRIDContainer) toUpdate;
      final boolean embeddedWasUpdated = ridContainer.addEntry(identity);

      if (!embeddedWasUpdated) {
        //noinspection unchecked
        return OIndexUpdateAction.nothing();
      } else {
        return OIndexUpdateAction.changed(toUpdate);
      }
    } else {
      if (toUpdate.add(identity)) {
        return OIndexUpdateAction.changed(toUpdate);
      } else {
        //noinspection unchecked
        return OIndexUpdateAction.nothing();
      }
    }
  }
}
