package com.orientechnologies.orient.core.metadata.schema;

import java.util.ArrayList;
import java.util.List;

public class OImmutableView extends OImmutableClass implements OView {

  private final int          updateIntervalSeconds;
  private final List<String> watchClasses;
  private       String       query;
  private       String       originRidField;
  private       boolean      updatable;

  public OImmutableView(OView view, OImmutableSchema schema) {
    super(view, schema);
    this.query = view.getQuery();
    this.updateIntervalSeconds = view.getUpdateIntervalSeconds();
    this.watchClasses = view.getWatchClasses() == null ? null : new ArrayList<>(view.getWatchClasses());
    this.originRidField = view.getOriginRidField();
    this.updatable = view.isUpdatable();
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
}
