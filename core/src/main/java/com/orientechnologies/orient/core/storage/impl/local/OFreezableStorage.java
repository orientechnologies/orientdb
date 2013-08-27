package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

/**
 * Storage that supports freeze operation.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 1.5.1
 */
public interface OFreezableStorage {

  /**
   * After this method finished it's execution, all threads that are going to perform data modifications in storage should wait till
   * {@link #release()} method will be called. This method will wait till all ongoing modifications will be finished.
   * 
   * @param throwException
   *          If <code>true</code> {@link com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *          exception will be thrown on call of methods that requires storage modification. Otherwise other threads will
   *          wait for {@link #release()} method call.
   */
  void freeze(boolean throwException);

  /**
   * After this method finished execution all threads that are waiting to perform data modifications in storage will be awaken and
   * will be allowed to continue their execution.
   */
  void release();
}
