package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2;

import com.orientechnologies.orient.core.index.engine.IndexEngineValidator;

public class IndexEngineValidatorIncrement<K>
    implements IndexEngineValidator<MultiValueEntry, Byte> {
  private final CellBTreeMultiValueV2Bucket<K> bucket;
  private final int index;

  public IndexEngineValidatorIncrement(CellBTreeMultiValueV2Bucket<K> bucketMultiValue, int index) {
    this.bucket = bucketMultiValue;
    this.index = index;
  }

  @Override
  public Object validate(MultiValueEntry k, Byte ov, Byte v) {
    if (ov != null) {
      return IndexEngineValidator.IGNORE;
    }

    bucket.incrementEntriesCount(index);
    return v;
  }
}
