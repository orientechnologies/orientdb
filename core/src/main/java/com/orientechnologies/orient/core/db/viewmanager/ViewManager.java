package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ViewManager {
  private final OrientDBInternal orientDB;
  private final String           dbName;

  ViewThread thread;

  ConcurrentMap<String, AtomicInteger> visitorsPerView;

  String lastUpdatedView = null;

  public ViewManager(OrientDBInternal orientDb, String dbName) {
    this.orientDB = orientDb;
    this.dbName = dbName;
  }

  public void start() {
    thread = new ViewThread(this, () -> orientDB.openNoAuthorization(dbName));
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
//    schema.reload();
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

  public synchronized void updateView(OView view, ODatabaseDocument db) {
    int cluster = db.addCluster(getNextClusterNameFor(view, db));
    String clusterName = db.getClusterNameById(cluster);
    OResultSet rs = db.query(view.getQuery());
    while (rs.hasNext()) {
      OResult item = rs.next();
      OElement newRow = copyElement(item, db);
      db.save(newRow, clusterName);
    }
    lockView(view);
    view.addClusterId(cluster);
    for (int i : view.getClusterIds()) {
      if (i != cluster) {
        view.removeClusterId(i);
        db.dropCluster(i, false);
      }
    }
    unlockView(view);
  }

  private void unlockView(OView view) {
    //TODO
  }

  private void lockView(OView view) {
    //TODO
  }

  private String getNextClusterNameFor(OView view, ODatabase db) {
    int i = 0;
    String viewName = view.getName();
    while (true) {
      String clusterName = viewName.toLowerCase(Locale.ENGLISH) + (i++);
      if (!db.getClusterNames().contains(clusterName)) {
        return clusterName;
      }
    }
  }

  private OElement copyElement(OResult item, ODatabaseDocument db) {
    OElement newRow = db.newElement();
    for (String prop : item.getPropertyNames()) {
      if (!prop.equalsIgnoreCase("@rid")) {
        newRow.setProperty(prop, item.getProperty(prop));
      }
    }
    return newRow;
  }

  public void updateViewAsync(String name, ViewCreationListener listener) {

    new Thread(() -> {
      ODatabaseDocument db = orientDB.openNoAuthorization(dbName);
//      db.activateOnCurrentThread();
      try {
        OView view = db.getMetadata().getSchema().getView(name);
        updateView(view, db);
        if (listener != null) {
          listener.afterCreate(name);
        }
      } catch (Exception e) {
//        e.printStackTrace();
        if (listener != null) {
          listener.onError(name, e);
        }
      } finally {
        db.close();
      }
    }).start();
  }
}