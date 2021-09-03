package com.orientechnologies.orient.core.storage.impl.local;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a
 *     href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 05/02/15
 */
public interface OCheckpointRequestListener {
  void requestCheckpoint();
}
