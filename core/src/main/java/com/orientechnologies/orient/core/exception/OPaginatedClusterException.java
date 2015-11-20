package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OPaginatedClusterException extends ODurableComponentException {
  public OPaginatedClusterException(OPaginatedClusterException exception) {
    super(exception);
  }

  public OPaginatedClusterException(String message, OPaginatedCluster component) {
    super(message, component);
  }
}
