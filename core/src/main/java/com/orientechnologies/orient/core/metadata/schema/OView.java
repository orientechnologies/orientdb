package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface for a class implementing a view schema object
 *
 * @author Matan Shukry
 * @since 29/9/2015
 */
public interface OView {
  String getName();
  OView setName(String iName);


  String getQuery();
  OView setQuery(String query);

  ODocument toStream();

  ORID getDocumentIdentity();
}
