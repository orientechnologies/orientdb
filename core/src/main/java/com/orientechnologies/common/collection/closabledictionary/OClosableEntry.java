package com.orientechnologies.common.collection.closabledictionary;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.atomic.AtomicReference;

class OClosableEntry<K, V> {
  @GuardedBy("lruLock")
  private OClosableEntry<K, V> next;

  @GuardedBy("lruLock")
  private OClosableEntry<K, V> prev;

  @GuardedBy("lruLock")
  public OClosableEntry<K, V> getNext() {
    return next;
  }

  @GuardedBy("lruLock")
  public void setNext(OClosableEntry<K, V> next) {
    this.next = next;
  }

  @GuardedBy("lruLock")
  public OClosableEntry<K, V> getPrev() {
    return prev;
  }

  @GuardedBy("lruLock")
  public void setPrev(OClosableEntry<K, V> prev) {
    this.prev = prev;
  }

  private final AtomicReference<V> item;

  private AtomicReference<State> state = new AtomicReference<State>(State.ALIVE);

  public OClosableEntry(V item) {
    this.item = new AtomicReference<V>(item);
  }

  public V get() {
    return item.get();
  }


  public boolean makeAcquired() {
    return state.compareAndSet(State.ALIVE, State.ACQUIRED);
  }

  public void makeRetired() {
    state.compareAndSet(State.ALIVE, State.RETIRED);
  }

  public void makeDead() {
    state.compareAndSet(State.RETIRED, State.DEAD);
  }

  public boolean isAlive() {
    final State s = state.get();
    return s.equals(State.OPEN) || s.equals(State.ACQUIRED);
  }

  public boolean isClosed() {
    return state.get().equals(State.CLOSED);
  }

  public boolean isRetired() {
    return state.get().equals(State.RETIRED);
  }

  public boolean isDead() {
    return state.get().equals(State.DEAD);
  }

  private enum State {
    OPEN, ACQUIRED, CLOSED, RETIRED, DEAD
  }
}
