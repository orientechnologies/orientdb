package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;

public class OViewDocument extends ODocument {

  private OView view;

  public OViewDocument(OView view) {
    this.view = view;
  }

  public OViewDocument(ODatabaseDocumentInternal database, int cluster) {
    view = database.getViewFromCluster(cluster);
  }

  @Override
  public OView getSchemaClass() {
    return view;
  }

  @Override
  protected OImmutableClass getImmutableSchemaClass() {
    if (view instanceof OImmutableClass) {
      return (OImmutableClass) view;
    } else {
      return (OImmutableClass) getImmutableSchema().getView(view.getName());
    }
  }

  @Override
  public String getClassName() {
    OView clazz = getSchemaClass();
    return clazz == null ? null : clazz.getName();
  }

  @Override
  public void setProperty(String iFieldName, Object iPropertyValue) {
    super.setProperty(iFieldName, iPropertyValue);
    if (view != null && view.isUpdatable()) {
      String originField = view.getOriginRidField();
      if (originField != null) {
        Object origin = getProperty(originField);
        if (origin instanceof ORID) {
          origin = ((ORID) origin).getRecord();
        }
        if (origin instanceof OElement) {
          ((OElement) origin).setProperty(iFieldName, iPropertyValue);
        }
      }
    }
  }
}
