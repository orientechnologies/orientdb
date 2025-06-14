package com.orientechnologies.orient.server.distributed;

/** Server status. */
public enum NODE_STATUS {
  /** The server was never started or the shutdown is complete. */
  OFFLINE,

  /** The server is STARTING. */
  STARTING,

  /** The server is ONLINE. */
  ONLINE,

  /** The server starts to merge to another cluster. */
  MERGING,

  /** The server is shutting down. */
  SHUTTINGDOWN
}
