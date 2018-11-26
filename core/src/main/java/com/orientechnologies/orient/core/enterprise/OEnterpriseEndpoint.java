package com.orientechnologies.orient.core.enterprise;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

public interface OEnterpriseEndpoint {
  void haSetDbStatus(ODatabaseDocument db, String nodeName, String status);

  void haSetRole(ODatabaseDocument db, String nodeName, String role);

  void haSetOwner(ODatabaseDocument db, String clusterName, String owner);
}
