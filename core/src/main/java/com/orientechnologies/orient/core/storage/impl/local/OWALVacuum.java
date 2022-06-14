package com.orientechnologies.orient.core.storage.impl.local;

final class OWALVacuum implements Runnable {

  private final OAbstractPaginatedStorage storage;

  public OWALVacuum(OAbstractPaginatedStorage storage) {
    this.storage = storage;
  }

  @Override
  public void run() {
    storage.runWALVacuum();
  }
}
