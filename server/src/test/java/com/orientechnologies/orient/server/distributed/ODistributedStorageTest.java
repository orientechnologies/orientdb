package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage;
import com.orientechnologies.orient.server.OServer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ODistributedStorageTest {

  @Before
  public void before() {
    if (!Orient.instance().isActive())
      Orient.instance().startup();
  }

  @Test
  public void testSupportedFreezeTrue() {
    OLocalPaginatedStorage storage = Mockito.mock(OLocalPaginatedStorage.class);
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), storage);

    ds.freeze(true);

    Mockito.verify(storage).freeze(true);
  }

  @Test
  public void testSupportedFreezeFalse() {
    OLocalPaginatedStorage storage = Mockito.mock(OLocalPaginatedStorage.class);
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), storage);

    ds.freeze(false);

    Mockito.verify(storage).freeze(false);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedFreeze() {
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), Mockito.mock(ODirectMemoryStorage.class));

    ds.freeze(false);
  }

  @Test
  public void testSupportedRelease() {
    OLocalPaginatedStorage storage = Mockito.mock(OLocalPaginatedStorage.class);
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), storage);

    ds.release();

    Mockito.verify(storage).release();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedRelease() {
    ODistributedStorage ds = new ODistributedStorage(Mockito.mock(OServer.class), Mockito.mock(ODirectMemoryStorage.class));

    ds.release();
  }
}
