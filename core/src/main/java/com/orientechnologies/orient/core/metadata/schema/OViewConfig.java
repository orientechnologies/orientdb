package com.orientechnologies.orient.core.metadata.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OViewConfig {
  /** default */
  public static String UPDATE_STRATEGY_BATCH = "batch";

  public static String UPDATE_STRATEGY_LIVE = "live";

  protected String name;
  protected String query;
  protected boolean updatable;
  protected List<OViewIndexConfig> indexes = new ArrayList<>();
  protected String updateStrategy = UPDATE_STRATEGY_BATCH;
  protected List<String> watchClasses = new ArrayList<>();
  protected List<String> nodes = null;
  protected int updateIntervalSeconds = 30;
  protected String originRidField = null;

  public OViewConfig(String name, String query) {
    this.name = name;
    this.query = query;
  }

  public OViewConfig copy() {
    OViewConfig result = new OViewConfig(this.name, this.query);
    result.updatable = this.updatable;
    for (OViewIndexConfig index : indexes) {
      OViewIndexConfig idx = result.addIndex(index.type, index.engine);
      idx.props = index.props.stream().map(v -> v.copy()).collect(Collectors.toList());
    }
    result.updateStrategy = this.updateStrategy;
    result.watchClasses = this.watchClasses == null ? null : new ArrayList<>(this.watchClasses);
    result.updateIntervalSeconds = this.updateIntervalSeconds;
    result.originRidField = this.originRidField;
    result.nodes = this.nodes == null ? null : new ArrayList<>(this.nodes);
    return result;
  }

  public OViewIndexConfig addIndex(String type, String engine) {
    OViewIndexConfig result = new OViewIndexConfig(type, engine);
    indexes.add(result);
    return result;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public boolean isUpdatable() {
    return updatable;
  }

  public void setUpdatable(boolean updatable) {
    this.updatable = updatable;
  }

  public List<OViewIndexConfig> getIndexes() {
    return indexes;
  }

  public String getUpdateStrategy() {
    return updateStrategy;
  }

  public void setUpdateStrategy(String updateStrategy) {
    this.updateStrategy = updateStrategy;
  }

  public List<String> getWatchClasses() {
    return watchClasses;
  }

  public void setWatchClasses(List<String> watchClasses) {
    this.watchClasses = watchClasses;
  }

  public int getUpdateIntervalSeconds() {
    return updateIntervalSeconds;
  }

  public void setUpdateIntervalSeconds(int updateIntervalSeconds) {
    this.updateIntervalSeconds = updateIntervalSeconds;
  }

  public String getOriginRidField() {
    return originRidField;
  }

  public void setOriginRidField(String originRidField) {
    this.originRidField = originRidField;
  }

  public List<String> getNodes() {
    return nodes;
  }

  public void setNodes(List<String> nodes) {
    this.nodes = nodes;
  }
}
