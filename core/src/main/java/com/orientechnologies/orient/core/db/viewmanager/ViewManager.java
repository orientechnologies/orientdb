package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ViewManager {
  private final Supplier<ODatabaseDocument> dbSupplier;

  ViewThread thread;

  ConcurrentMap<String, AtomicInteger> visitorsPerView;

  String lastUpdatedView = null;

  public ViewManager(Supplier<ODatabaseDocument> dbSupplier) {
    this.dbSupplier = dbSupplier;
  }

  public void start() {
    thread = new ViewThread(this, dbSupplier);
    thread.start();
  }

  public void close() {
    try {
      thread.finish();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public synchronized OView getNextViewToUpdate(ODatabase db) {
    OSchema schema = db.getMetadata().getSchema();
    schema.reload();
    List<String> names = schema.getViews().stream().map(x -> x.getName()).sorted().collect(Collectors.toList());
    if (names.isEmpty()) {
      return null;
    }
    for (String name : names) {
      if (lastUpdatedView == null || name.compareTo(lastUpdatedView) > 0) {
        lastUpdatedView = name;
        return schema.getView(name);
      }
    }

    lastUpdatedView = null;
    return null;
  }
}
