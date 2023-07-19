package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;

public class OQueryDatabaseState {

  private OResultSet resultSet = null;
  private List<Integer> usedClusters = new ArrayList<>();
  private List<String> usedIndexes = new ArrayList<>();

  public OQueryDatabaseState() {}

  public OQueryDatabaseState(OResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public void setResultSet(OResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public OResultSet getResultSet() {
    return resultSet;
  }

  public void close(ODatabaseDocumentInternal database) {
    if (resultSet != null) {
      resultSet.close();
    }
    this.closeInternal(database);
  }

  public void closeInternal(ODatabaseDocumentInternal database) {
    if (database.isRemote()) {
      return;
    }
    ViewManager views = database.getSharedContext().getViewManager();
    for (int cluster : this.usedClusters) {
      views.endUsingViewCluster(cluster);
    }
    this.usedClusters.clear();
    for (String index : this.usedIndexes) {
      views.endUsingViewIndex(index);
    }
    this.usedIndexes.clear();
  }

  public void addViewUseCluster(int clusterId) {
    this.usedClusters.add(clusterId);
  }

  public void addViewUseIndex(String index) {
    this.usedIndexes.add(index);
  }
}
