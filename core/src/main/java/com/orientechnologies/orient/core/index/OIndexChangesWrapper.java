package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OIndexIsRebuildingException;

import java.util.Map;
import java.util.Set;

/**
 * Wrapper which is used to detect whether the cursor is working with index
 * which is being rebuilt at the moment.
 * <p>
 * If such situation is detected any call to any method throws {@link OIndexIsRebuildingException}
 *
 * @see OIndexAbstract#getRebuildVersion()
 */
public class OIndexChangesWrapper implements OIndexCursor {
  private final OIndex<?>    source;
  private final OIndexCursor delegate;
  private final long         indexVersion;

  /**
   * Wraps courser only if it is not already wrapped.
   *
   * @param source Index which is used to create given cursor.
   * @param cursor Cursor to wrap.
   * @return Wrapped cursor.
   */
  public static OIndexCursor wrap(OIndex<?> source, OIndexCursor cursor) {
    if (cursor instanceof OIndexChangesWrapper)
      return cursor;

    return new OIndexChangesWrapper(source, cursor);
  }

  private OIndexChangesWrapper(OIndex<?> source, OIndexCursor delegate) {
    this.source = source;
    this.delegate = delegate;

    indexVersion = source.getRebuildVersion();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map.Entry<Object, OIdentifiable> nextEntry() {
    if (source.isRebuilding())
      throwRebuildException();

    final Map.Entry<Object, OIdentifiable> entry = delegate.nextEntry();

    if (source.getRebuildVersion() != indexVersion)
      throwRebuildException();

    return entry;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<OIdentifiable> toValues() {
    if (source.isRebuilding())
      throwRebuildException();

    final Set<OIdentifiable> values = delegate.toValues();

    if (source.getRebuildVersion() != indexVersion)
      throwRebuildException();

    return values;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
    if (source.isRebuilding())
      throwRebuildException();

    final Set<Map.Entry<Object, OIdentifiable>> entries = delegate.toEntries();

    if (source.getRebuildVersion() != indexVersion)
      throwRebuildException();

    return entries;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Object> toKeys() {
    if (source.isRebuilding())
      throwRebuildException();

    final Set<Object> keys = delegate.toKeys();

    if (source.getRebuildVersion() != indexVersion)
      throwRebuildException();

    return keys;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setPrefetchSize(int prefetchSize) {
    delegate.setPrefetchSize(prefetchSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {
    if (source.isRebuilding())
      throwRebuildException();

    final boolean isNext = delegate.hasNext();

    if (source.getRebuildVersion() != indexVersion)
      throwRebuildException();

    return isNext;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OIdentifiable next() {
    if (source.isRebuilding())
      throwRebuildException();

    final OIdentifiable next = delegate.next();

    if (source.getRebuildVersion() != indexVersion)
      throwRebuildException();

    return next;
  }

  private void throwRebuildException() {
    throw new OIndexIsRebuildingException("Index " + source.getName() + " is rebuilding at the moment and can not be used");
  }
}
