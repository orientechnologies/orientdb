package com.orientechnologies.orient.server.distributed;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;
import com.orientechnologies.orient.server.OServer;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class ODistributedStorageTest {
  @Test
  public void testSupportedFreezeTrue() {
    OStorageLocal storage = Mockito.mock(OStorageLocal.class);
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), storage);

    ds.freeze(true);

    Mockito.verify(storage).freeze(true);
  }

  @Test
  public void testSupportedFreezeFalse() {
    OStorageLocal storage = Mockito.mock(OStorageLocal.class);
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), storage);

    ds.freeze(false);

    Mockito.verify(storage).freeze(false);
  }

  @Test(expectedExceptions = { UnsupportedOperationException.class })
  public void testUnsupportedFreeze() {
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), Mockito.mock(OStorageMemory.class));

    ds.freeze(false);
  }

  @Test
  public void testSupportedRelease() {
    OStorageLocal storage = Mockito.mock(OStorageLocal.class);
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), storage);

    ds.release();

    Mockito.verify(storage).release();
  }

  @Test(expectedExceptions = { UnsupportedOperationException.class })
  public void testUnsupportedRelease() {
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), Mockito.mock(OStorageMemory.class));

    ds.release();
  }
}
