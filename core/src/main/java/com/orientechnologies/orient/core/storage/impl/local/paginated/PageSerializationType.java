package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTableBucket;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v3.ODirectoryFirstPageV3;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v3.ODirectoryPageV3;
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
      return new OBonsaiBucketAbstractV2(cacheEntry, 1024);
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
  }, HASH_TABLE_BUCKET {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new OHashTableBucket<>(cacheEntry);
    }
  }, HASH_NULL_BUCKET {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new com.orientechnologies.orient.core.storage.index.hashindex.local.ONullBucket<>(cacheEntry);
    }
  }, HASH_DIRECTORY_PAGE {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new ODirectoryPageV3(cacheEntry);
    }
  }, HASH_DIRECTORY_FIRST_PAGE {
    @Override
    public ODurablePage pageInstance(OCacheEntry cacheEntry) {
      return new ODirectoryFirstPageV3(cacheEntry);
    }
  };

  public abstract ODurablePage pageInstance(OCacheEntry cacheEntry);
}
