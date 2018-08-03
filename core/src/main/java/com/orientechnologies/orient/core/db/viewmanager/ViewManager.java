package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ViewManager {
  private final OrientDBInternal orientDB;
  private final String           dbName;

  ViewThread thread;

  /**
   * To retain clusters that are being used in queries until the queries are closed.
   * <p>
   * view -> cluster -> number of visitors
   */
  ConcurrentMap<Integer, AtomicInteger> viewCluserVisitors = new ConcurrentHashMap<>();
  List<Integer>                         clustersToDrop     = Collections.synchronizedList(new ArrayList<>());

  Map<String, Long> lastUpdateTimestampForView = new HashMap<>();

  String lastUpdatedView = null;

  public ViewManager(OrientDBInternal orientDb, String dbName) {
    this.orientDB = orientDb;
    this.dbName = dbName;
    init();
  }

  protected void init() {

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

  public synchronized void cleanUnusedViewClusters(ODatabaseDocument db) {
    List<Integer> toRemove = new ArrayList<>();
    for (Integer cluster : clustersToDrop) {
      AtomicInteger visitors = viewCluserVisitors.get(cluster);
      if (visitors == null || visitors.get() <= 0) {
        toRemove.add(cluster);
      }
    }
    for (Integer cluster : toRemove) {
      viewCluserVisitors.remove(cluster);
      clustersToDrop.remove(cluster);
      db.dropCluster(cluster, false);
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
      if (!isUpdateExpiredFor(name, db)) {
        continue;
      }
      if (lastUpdatedView == null || name.compareTo(lastUpdatedView) > 0) {
        lastUpdatedView = name;
        return schema.getView(name);
      }
    }

    lastUpdatedView = null;
    return null;
  }

  private boolean isUpdateExpiredFor(String viewName, ODatabase db) {
    Long lastUpdate = lastUpdateTimestampForView.get(viewName);
    if (lastUpdate == null) {
      return true;
    }
    OView view = db.getMetadata().getSchema().getView(viewName);
    int updateInterval = view.getUpdateIntervalSeconds();
    return lastUpdate + (updateInterval * 1000) < System.currentTimeMillis();
  }

  public synchronized void updateView(OView view, ODatabaseDocument db) {

    lastUpdateTimestampForView.put(view.getName(), System.currentTimeMillis());

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
        clustersToDrop.add(i);
        viewCluserVisitors.put(i, new AtomicInteger(0));
        view.removeClusterId(i);
      }
    }
    unlockView(view);
    cleanUnusedViewClusters(db);
  }

  private synchronized void unlockView(OView view) {
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

  public synchronized void startUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      item = new AtomicInteger(0);
      viewCluserVisitors.put(cluster, item);
    }
    item.incrementAndGet();
  }

  public synchronized void endUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      return;
    }
    item.decrementAndGet();
  }
}