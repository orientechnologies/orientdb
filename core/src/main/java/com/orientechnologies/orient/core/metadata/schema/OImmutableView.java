package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class OImmutableView extends OImmutableClass implements OView {

  private final int updateIntervalSeconds;
  private final List<String> watchClasses;
  private final List<String> nodes;
  private final List<OViewIndexConfig> requiredIndexesInfo;
  private String query;
  private String originRidField;
  private boolean updatable;
  private String updateStrategy;
  private Set<String> activeIndexNames;
  private long lastRefreshTime;

  public OImmutableView(OView view, OImmutableSchema schema) {
    super(view, schema);
    this.query = view.getQuery();
    this.updateIntervalSeconds = view.getUpdateIntervalSeconds();
    this.watchClasses =
        view.getWatchClasses() == null ? null : new ArrayList<>(view.getWatchClasses());
    this.originRidField = view.getOriginRidField();
    this.updatable = view.isUpdatable();
    this.nodes = view.getNodes() == null ? null : new ArrayList<>(view.getNodes());
    this.requiredIndexesInfo =
        view.getRequiredIndexesInfo() == null
            ? null
            : new ArrayList<>(view.getRequiredIndexesInfo());
    this.updateStrategy = view.getUpdateStrategy();
    this.activeIndexNames = view.getActiveIndexNames();
    this.lastRefreshTime = view.getLastRefreshTime();
  }

  public void getRawClassIndexes(final Collection<OIndex> indexes) {
    ODatabaseDocumentInternal database = getDatabase();
    OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    for (String indexName : activeIndexNames) {
      OIndex index = indexManager.getIndex(database, indexName);
      indexes.add(index);
    }
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public int getUpdateIntervalSeconds() {
    return updateIntervalSeconds;
  }

  @Override
  public List<String> getWatchClasses() {
    return watchClasses;
  }

  public String getOriginRidField() {
    return originRidField;
  }

  public boolean isUpdatable() {
    return updatable;
  }

  @Override
  public List<String> getNodes() {
    return nodes;
  }

  @Override
  public List<OViewIndexConfig> getRequiredIndexesInfo() {
    return requiredIndexesInfo;
  }

  @Override
  public String getUpdateStrategy() {
    return updateStrategy;
  }

  @Override
  public long count(boolean isPolymorphic) {
    return getDatabase().countView(getName());
  }

  @Override
  public Set<String> getActiveIndexNames() {
    return activeIndexNames;
  }

  @Override
  public long getLastRefreshTime() {
    return lastRefreshTime;
  }
}
