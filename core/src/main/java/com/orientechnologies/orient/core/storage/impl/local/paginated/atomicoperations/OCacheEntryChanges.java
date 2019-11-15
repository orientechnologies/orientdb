package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.chm.LRUList;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.util.List;

/**
 * Created by tglman on 23/06/16.
 */
public class OCacheEntryChanges implements OCacheEntry {

  protected       OCacheEntry delegate;
  protected final OWALChanges changes = new OWALPageChangesPortion();

  protected boolean isNew;

  private OLogSequenceNumber changeLSN;

  protected boolean verifyCheckSum;

  public OCacheEntryChanges(final OCacheEntry entry) {
    delegate = entry;
  }

  @SuppressWarnings("WeakerAccess")
  public OCacheEntryChanges(final boolean verifyCheckSum) {
    this.verifyCheckSum = verifyCheckSum;
  }

  @Override
  public OCachePointer getCachePointer() {
    return delegate.getCachePointer();
  }

  @Override
  public void clearCachePointer() {
    delegate.clearCachePointer();
  }

  @Override
  public void setCachePointer(final OCachePointer cachePointer) {
    delegate.setCachePointer(cachePointer);
  }

  @Override
  public long getFileId() {
    return delegate.getFileId();
  }

  @Override
  public int getPageIndex() {
    return delegate.getPageIndex();
  }

  @Override
  public void acquireExclusiveLock() {
    delegate.acquireExclusiveLock();
  }

  @Override
  public void releaseExclusiveLock() {
    delegate.releaseExclusiveLock();
  }

  @Override
  public void acquireSharedLock() {
    delegate.acquireSharedLock();
  }

  @Override
  public void releaseSharedLock() {
    delegate.releaseSharedLock();
  }

  @Override
  public int getUsagesCount() {
    return delegate.getUsagesCount();
  }

  @Override
  public void incrementUsages() {
    delegate.incrementUsages();
  }

  @Override
  public boolean isLockAcquiredByCurrentThread() {
    return delegate.isLockAcquiredByCurrentThread();
  }

  @Override
  public void decrementUsages() {
    delegate.decrementUsages();
  }

  @Override
  public OWALChanges getChanges() {
    return changes;
  }

  public void setDelegate(final OCacheEntry delegate) {
    this.delegate = delegate;
  }

  public OCacheEntry getDelegate() {
    return delegate;
  }

  @Override
  public OLogSequenceNumber getEndLSN() {
    return delegate.getEndLSN();
  }

  @Override
  public void setEndLSN(final OLogSequenceNumber endLSN) {
    delegate.setEndLSN(endLSN);
  }

  @Override
  public boolean acquireEntry() {
    return delegate.acquireEntry();
  }

  @Override
  public void releaseEntry() {
    delegate.releaseEntry();
  }

  @Override
  public boolean isReleased() {
    return delegate.isReleased();
  }

  @Override
  public boolean isAlive() {
    return delegate.isAlive();
  }

  @Override
  public boolean freeze() {
    return delegate.freeze();
  }

  @Override
  public boolean isFrozen() {
    return delegate.isFrozen();
  }

  @Override
  public void makeDead() {
    delegate.makeDead();
  }

  @Override
  public boolean isDead() {
    return delegate.isDead();
  }

  @Override
  public boolean isNewlyAllocatedPage() {
    return delegate.isNewlyAllocatedPage();
  }

  @Override
  public void markAllocated() {
    delegate.markAllocated();
  }

  @Override
  public void clearAllocationFlag() {
    delegate.clearAllocationFlag();
  }

  @Override
  public List<PageOperationRecord> getPageOperations() {
    return delegate.getPageOperations();
  }

  @Override
  public void clearPageOperations() {
    delegate.clearPageOperations();
  }

  @Override
  public void addPageOperationRecord(PageOperationRecord pageOperationRecord) {
    delegate.addPageOperationRecord(pageOperationRecord);
  }

  @Override
  public OCacheEntry getNext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OCacheEntry getPrev() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPrev(final OCacheEntry prev) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNext(final OCacheEntry next) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setContainer(final LRUList lruList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public LRUList getContainer() {
    throw new UnsupportedOperationException();
  }

  OLogSequenceNumber getChangeLSN() {
    return changeLSN;
  }

  void setChangeLSN(final OLogSequenceNumber walLSN) {
    this.changeLSN = walLSN;
  }
}
