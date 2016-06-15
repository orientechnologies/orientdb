package com.orientechnologies.common.collection.closabledictionary;

import java.util.concurrent.atomic.AtomicReference;

public class OClosableEntry<K, E> {
  private final AtomicReference<E> item;

  private AtomicReference<State> state = new AtomicReference<State>(State.ALIVE);

  public OClosableEntry(E item) {
    this.item = new AtomicReference<E>(item);
  }

  public E get() {
    return item.get();
  }

  public void makeRetired() {
    state.compareAndSet(State.ALIVE, State.RETIRED);
  }

  public void makeDead() {
    state.compareAndSet(State.RETIRED, State.DEAD);
  }

  public boolean isAlive() {
    return state.get().equals(State.ALIVE);
  }

  public boolean isRetired() {
    return state.get().equals(State.RETIRED);
  }

  public boolean isDead() {
    return state.get().equals(State.DEAD);
  }

  private enum State {
    ALIVE, RETIRED, DEAD
  }
}
