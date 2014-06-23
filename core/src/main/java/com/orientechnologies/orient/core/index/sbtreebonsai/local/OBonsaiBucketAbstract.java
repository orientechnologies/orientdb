package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * A base class for bonsai buckets. Bonsai bucket size is usually less than page size and one page could contain multiple bonsai
 * buckets.
 * 
 * Adds methods to read and write bucket pointers.
 * 
 * @see com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer
 * @see com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OBonsaiBucketAbstract extends ODurablePage {
  public OBonsaiBucketAbstract(ODirectMemoryPointer pagePointer, TrackMode trackMode) {
    super(pagePointer, trackMode);
  }

  /**
   * Write a bucket pointer to specific location.
   * 
   * @param pageOffset
   *          where to write
   * @param value
   *          - pointer to write
   * @throws IOException
   */
  protected void setBucketPointer(int pageOffset, OBonsaiBucketPointer value) throws IOException {
    setLongValue(pageOffset, value.getPageIndex());
    setIntValue(pageOffset + OLongSerializer.LONG_SIZE, value.getPageOffset());
  }

  /**
   * Read bucket pointer from page.
   * 
   * @param offset
   *          where the pointer should be read from
   * @return bucket pointer
   */
  protected OBonsaiBucketPointer getBucketPointer(int offset) {
    final long pageIndex = getLongValue(offset);
    final int pageOffset = getIntValue(offset + OLongSerializer.LONG_SIZE);
    return new OBonsaiBucketPointer(pageIndex, pageOffset);
  }
}
