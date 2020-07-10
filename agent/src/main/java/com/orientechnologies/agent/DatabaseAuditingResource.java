package com.orientechnologies.agent;

import com.orientechnologies.orient.core.metadata.security.ORule;

/** Created by Enrico Risa on 09/04/15. */
public class DatabaseAuditingResource extends ORule.ResourceGeneric {

  public static final String DATABASE_AUDITING = "database.auditing";
  public static final String AUDITING = "AUDITING";

  public DatabaseAuditingResource() {
    super(AUDITING, DATABASE_AUDITING);
  }
}
