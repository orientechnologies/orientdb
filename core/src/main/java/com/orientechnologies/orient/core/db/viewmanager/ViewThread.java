package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OView;

import java.util.function.Supplier;

public class ViewThread extends Thread {

  private final Supplier<ODatabaseDocument> dbSupplier;
  private final ViewManager                 viewManager;

//  private ODatabaseDocument db;

  boolean interrupted = false;

  public ViewThread(ViewManager mgr, Supplier<ODatabaseDocument> dbSupplier) {
    this.setDaemon(true);
    this.dbSupplier = dbSupplier;
    this.viewManager = mgr;
  }

  @Override
  public void run() {
    while (!interrupted) {
      ODatabaseDocument db = null;
      try {
        db = dbSupplier.get();

        updateViews(db);
      } catch (ODatabaseException ex) {
        return;
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        db.close();
      }
      try {
        Thread.sleep(5_000);
      } catch (InterruptedException e) {
      }
    }
  }

  private void updateViews(ODatabaseDocument db) {
    try {
      viewManager.cleanUnusedViewClusters(db);
      OView view = viewManager.getNextViewToUpdate(db);
      while (view != null) {
        if (interrupted) {
          return;
        }
        viewManager.updateView(view, db);

        view = viewManager.getNextViewToUpdate(db);
      }
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Failed to update views");
      e.printStackTrace();
    }
  }

  public void finish() throws InterruptedException {
    interrupted = true;
    this.interrupt();
  }
}
