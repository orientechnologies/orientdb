package com.orientechnologies.orient.core.index.multivalue;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.ODefaultIndexFactory;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public final class OMultivalueIndexKeyUpdaterImpl implements OIndexKeyUpdater<Object> {
  private final ORID identity;
  private final String indexName;
  private final boolean mixedContainer;
  private final OAbstractPaginatedStorage storage;

  public OMultivalueIndexKeyUpdaterImpl(
      ORID identity,
      String valueContainerAlgorithm,
      int binaryFormatVersion,
      String indexName,
      OAbstractPaginatedStorage storage) {
    this.identity = identity;
    this.indexName = indexName;
    this.storage = storage;
    if (ODefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
      if (binaryFormatVersion >= 13) {
        mixedContainer = true;
      } else {
        mixedContainer = false;
      }
    } else {
      throw new IllegalStateException("MVRBTree is not supported any more");
    }
  }

  @Override
  public OIndexUpdateAction<Object> update(Object oldValue, AtomicLong bonsayFileId) {
    Set<OIdentifiable> toUpdate = (Set<OIdentifiable>) oldValue;
    if (toUpdate == null) {
      if (mixedContainer) {
        toUpdate = new OMixedIndexRIDContainer(indexName, bonsayFileId, storage);
      } else {
        toUpdate = new OIndexRIDContainer(indexName, true, bonsayFileId, storage);
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
