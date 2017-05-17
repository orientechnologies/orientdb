package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by tglman on 17/05/17.
 */
public class ORemoteLiveQueryPushTest {

  private static class MockLiveListener implements OLiveQueryResultListener {
    public int countCreate = 0;
    public int countUpdate = 0;
    public int countDelete = 0;
    public boolean end;

    @Override
    public void onCreate(OResult data) {
      countCreate++;
    }

    @Override
    public void onUpdate(OResult before, OResult after) {
      countUpdate++;
    }

    @Override
    public void onDelete(OResult data) {
      countDelete++;
    }

    @Override
    public void onError() {

    }

    @Override
    public void onEnd() {
      assertFalse(end);
      end = true;
    }
  }

  private OStorageRemote storage;

  @Mock
  private ORemoteConnectionManager connectionManager;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    storage = new OStorageRemote("none", "", connectionManager);

  }

  @Test
  public void testLiveEvents() {
    MockLiveListener mock = new MockLiveListener();
    storage.registerLiveListener(10, mock);
    List<OLiveQueryResult> events = new ArrayList<>();
    events.add(new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, new OResultInternal(), null));
    events.add(new OLiveQueryResult(OLiveQueryResult.UPDATE_EVENT, new OResultInternal(), new OResultInternal()));
    events.add(new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, new OResultInternal(), null));

    OLiveQueryPushRequest request = new OLiveQueryPushRequest(10, OLiveQueryPushRequest.END, events);
    request.execute(storage);
    assertEquals(mock.countCreate, 1);
    assertEquals(mock.countUpdate, 1);
    assertEquals(mock.countDelete, 1);
  }

}
