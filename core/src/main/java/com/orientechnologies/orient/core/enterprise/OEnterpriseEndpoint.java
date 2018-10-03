package com.orientechnologies.orient.core.enterprise;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

public interface OEnterpriseEndpoint {
  void haSetDbStatus(ODatabaseDocument db, String status);

  void haSetRole(ODatabaseDocument db, String status);

  void haSetOwner(ODatabaseDocument db, String status);
}
