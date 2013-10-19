package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OBonsaiBucketAbstract extends ODurablePage {
  public OBonsaiBucketAbstract(ODirectMemoryPointer pagePointer, TrackMode trackMode) {
    super(pagePointer, trackMode);
  }

  protected void setBucketPointer(int pageOffset, OBonsaiBucketPointer value) throws IOException {
    setLongValue(pageOffset, value.getPageIndex());
    setIntValue(pageOffset + OLongSerializer.LONG_SIZE, value.getPageOffset());
  }

  protected OBonsaiBucketPointer getBucketPointer(int freePointer) {
    final long pageIndex = getLongValue(freePointer);
    final int pageOffset = getIntValue(freePointer + OLongSerializer.LONG_SIZE);
    return new OBonsaiBucketPointer(pageIndex, pageOffset);
  }
}
