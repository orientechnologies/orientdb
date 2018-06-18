package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.ONullBucket;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v2.OBonsaiBucketAbstractV2;

public enum PageSerializationType {
  GENERIC {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new ODurablePage(cacheEntry);
    }
  }, CLUSTER_PAGE {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new OClusterPage(cacheEntry);
    }
  }, SBTREE_BUCKET {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new OSBTreeBucket<>(cacheEntry);
    }
  }, SBTREE_BONSAI_BUCKET {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new OBonsaiBucketAbstractV2(cacheEntry);
    }
  }, CLUSTER_POSITION_MAP_BUCKET {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new OClusterPositionMapBucket(cacheEntry);
    }
  }, PAGINATED_CLUSTER_STATE {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new OPaginatedClusterState(cacheEntry);
    }
  }, SBTREE_NULL_BUCKET {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new ONullBucket<>(cacheEntry);
    }
  };

  public abstract ODurablePage pageInstance(OCacheEntry cacheEntry);
}
