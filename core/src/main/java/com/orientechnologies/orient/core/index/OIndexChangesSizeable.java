package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.util.OSizeable;

/**
 * Adds support of {@link OSizeable} interface if index cursor implements it.
 */
class OIndexChangesSizeable extends OIndexChangesWrapper implements OSizeable {
  public OIndexChangesSizeable(OIndex<?> source, OIndexCursor delegate, long indexRebuildVersion) {
    super(source, delegate, indexRebuildVersion);
  }

  @Override
  public int size() {
    if (source.isRebuilding())
      throwRebuildException();

    final int size = ((OSizeable) delegate).size();

    if (source.getRebuildVersion() != indexRebuildVersion)
      throwRebuildException();

    return size;
  }
}
