package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Locale;
import java.util.function.Supplier;

public class ViewThread extends Thread {

  private final Supplier<ODatabaseDocument> dbSupplier;
  private final ViewManager                 viewManager;

  private ODatabaseDocument db;

  boolean        interrupted = false;


  public ViewThread(ViewManager mgr, Supplier<ODatabaseDocument> dbSupplier) {
    this.setDaemon(true);
    this.dbSupplier = dbSupplier;
    this.viewManager = mgr;
  }

  @Override
  public void run() {
    this.db = dbSupplier.get();
    this.db.activateOnCurrentThread();
    try {
      while (!interrupted) {
        updateViews();
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      db.close();
    }
  }

  private void updateViews() {
    try {
      OView view = viewManager.getNextViewToUpdate(db);
      while (view != null) {
        if (interrupted) {
          return;
        }
        int cluster = db.addCluster(getNextClusterNameFor(view, db));
        String clusterName = db.getClusterNameById(cluster);
        OResultSet rs = db.query(view.getQuery());
        while (rs.hasNext()) {
          if (interrupted) {
            return;
          }
          OResult item = rs.next();
          OElement newRow = copyElement(item);
          newRow.save(clusterName);
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

        view = viewManager.getNextViewToUpdate(db);
      }
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Failed to update views");
    }
  }

  private OElement copyElement(OResult item) {
    OElement newRow = db.newElement();
    for (String prop : item.getPropertyNames()) {
      if (!prop.equalsIgnoreCase("@rid")) {
        newRow.setProperty(prop, item.getProperty(prop));
      }
    }
    return newRow;
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

  public void finish() throws InterruptedException {
    interrupted = true;
    this.interrupt();
  }
}
