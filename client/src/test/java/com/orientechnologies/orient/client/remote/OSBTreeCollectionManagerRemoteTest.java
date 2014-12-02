package com.orientechnologies.orient.client.remote;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeCollectionManagerRemoteTest {

  private static final int                  EXPECTED_FILE_ID      = 17;
  private static final OBonsaiBucketPointer EXPECTED_ROOT_POINTER = new OBonsaiBucketPointer(11, 118);
  private static final int                  EXPECTED_CLUSTER_ID   = 3;

  @Mock
  private OCollectionNetworkSerializer      networkSerializerMock;
  @Mock
  private ODatabaseDocumentInternal         dbMock;
  @Mock
  private OStorageRemote                    storageMock;
  @Mock
  private OChannelBinaryAsynchClient        clientMock;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test(enabled = false)
  public void testCreateTree() throws Exception {
    OSBTreeCollectionManagerRemote remoteManager = new OSBTreeCollectionManagerRemote(networkSerializerMock);
    ODatabaseRecordThreadLocal.INSTANCE.set(dbMock);

    when(dbMock.getStorage()).thenReturn(storageMock);
    when(storageMock.getUnderlying()).thenReturn(storageMock);
    when(storageMock.beginRequest(eq(OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI))).thenReturn(clientMock);
    when(networkSerializerMock.readCollectionPointer(Mockito.<OChannelBinaryAsynchClient> any())).thenReturn(
        new OBonsaiCollectionPointer(EXPECTED_FILE_ID, EXPECTED_ROOT_POINTER));

    OSBTreeBonsaiRemote<OIdentifiable, Integer> tree = remoteManager.createTree(EXPECTED_CLUSTER_ID);

    assertNotNull(tree);
    assertEquals(tree.getFileId(), EXPECTED_FILE_ID);
    assertEquals(tree.getRootBucketPointer(), EXPECTED_ROOT_POINTER);

    verify(dbMock).getStorage();
    verifyNoMoreInteractions(dbMock);

    verify(storageMock).getUnderlying();
    verify(storageMock).beginRequest(eq(OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI));
    verify(clientMock).writeInt(eq(EXPECTED_CLUSTER_ID));
    verify(storageMock).endRequest(Matchers.same(clientMock));
    verify(storageMock).beginResponse(Matchers.same(clientMock));
    verify(networkSerializerMock).readCollectionPointer(Matchers.same(clientMock));
    verify(storageMock).endResponse(Matchers.same(clientMock));
    verifyNoMoreInteractions(storageMock);
  }

  @Test
  public void testLoadTree() throws Exception {
    OSBTreeCollectionManagerRemote remoteManager = new OSBTreeCollectionManagerRemote(networkSerializerMock);

    OSBTreeBonsai<OIdentifiable, Integer> tree = remoteManager.loadTree(new OBonsaiCollectionPointer(EXPECTED_FILE_ID,
        EXPECTED_ROOT_POINTER));

    assertNotNull(tree);
    assertEquals(tree.getFileId(), EXPECTED_FILE_ID);
    assertEquals(tree.getRootBucketPointer(), EXPECTED_ROOT_POINTER);
  }
}
