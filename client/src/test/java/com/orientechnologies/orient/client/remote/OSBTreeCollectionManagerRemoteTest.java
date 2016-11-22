package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeCollectionManagerRemoteTest {

  private static final int                  EXPECTED_FILE_ID      = 17;
  private static final OBonsaiBucketPointer EXPECTED_ROOT_POINTER = new OBonsaiBucketPointer(11, 118);
  private static final int                  EXPECTED_CLUSTER_ID   = 3;

  @Mock
  private OCollectionNetworkSerializer networkSerializerMock;
  @Mock
  private ODatabaseDocumentInternal    dbMock;
  @Mock
  private OStorageRemote               storageMock;
  @Mock
  private OChannelBinaryAsynchClient   clientMock;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  @Ignore
  public void testCreateTree() throws Exception {
    OSBTreeCollectionManagerRemote remoteManager = new OSBTreeCollectionManagerRemote(storageMock, networkSerializerMock);
    ODatabaseRecordThreadLocal.INSTANCE.set(dbMock);

    when(dbMock.getStorage()).thenReturn(storageMock);
    when(storageMock.getUnderlying()).thenReturn(storageMock);
    when(storageMock
        .beginRequest(Mockito.any(OChannelBinaryAsynchClient.class), eq(OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI),
            Mockito.any(OStorageRemoteSession.class))).thenReturn(clientMock);
    when(networkSerializerMock.readCollectionPointer(Mockito.<OChannelBinaryAsynchClient>any()))
        .thenReturn(new OBonsaiCollectionPointer(EXPECTED_FILE_ID, EXPECTED_ROOT_POINTER));

    OSBTreeBonsaiRemote<OIdentifiable, Integer> tree = remoteManager.createTree(EXPECTED_CLUSTER_ID);

    assertNotNull(tree);
    assertEquals(tree.getFileId(), EXPECTED_FILE_ID);
    assertEquals(tree.getRootBucketPointer(), EXPECTED_ROOT_POINTER);

    verify(dbMock).getStorage();
    verifyNoMoreInteractions(dbMock);

    verify(storageMock).getUnderlying();
    verify(storageMock)
        .beginRequest(Mockito.any(OChannelBinaryAsynchClient.class), eq(OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI),
            Mockito.any(OStorageRemoteSession.class));
    verify(clientMock).writeInt(eq(EXPECTED_CLUSTER_ID));
    verify(storageMock).endRequest(Matchers.same(clientMock));
    verify(storageMock).beginResponse(Matchers.same(clientMock), Mockito.any(OStorageRemoteSession.class));
    verify(networkSerializerMock).readCollectionPointer(Matchers.same(clientMock));
    verify(storageMock).endResponse(Matchers.same(clientMock));
    verifyNoMoreInteractions(storageMock);
  }

  @Test
  public void testLoadTree() throws Exception {
    OSBTreeCollectionManagerRemote remoteManager = new OSBTreeCollectionManagerRemote(storageMock, networkSerializerMock);

    OSBTreeBonsai<OIdentifiable, Integer> tree = remoteManager
        .loadTree(new OBonsaiCollectionPointer(EXPECTED_FILE_ID, EXPECTED_ROOT_POINTER));

    assertNotNull(tree);
    assertEquals(tree.getFileId(), EXPECTED_FILE_ID);
    assertEquals(tree.getRootBucketPointer(), EXPECTED_ROOT_POINTER);
  }
}
