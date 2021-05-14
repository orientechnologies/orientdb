package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.id.ORecordId;
import org.junit.Assert;
import org.junit.Test;

public class OTxIndexChangesListTest {

  @Test
  public void testEmpty() {
    OTxIndexChangesList list = new OTxIndexChangesList();
    Assert.assertEquals(0, list.size());
    try {
      Assert.assertFalse(list.iterator().hasNext());
      list.iterator().next();
      Assert.fail();
    } catch (IllegalStateException ex) {

    }
    try {
      list.get(0);
      Assert.fail();
    } catch (IndexOutOfBoundsException ex) {

    }
  }

  @Test
  public void testAddRemove() {

    OTxIndexChangesList list = new OTxIndexChangesList();
    OTransactionIndexChangesPerKey temp = new OTransactionIndexChangesPerKey(null);

    list.add(
        temp.createEntryInternal(new ORecordId(12, 0), OTransactionIndexChanges.OPERATION.PUT));
    list.add(
        temp.createEntryInternal(new ORecordId(12, 1), OTransactionIndexChanges.OPERATION.PUT));
    list.add(
        temp.createEntryInternal(new ORecordId(12, 2), OTransactionIndexChanges.OPERATION.PUT));
    list.add(
        temp.createEntryInternal(new ORecordId(12, 3), OTransactionIndexChanges.OPERATION.PUT));
    Assert.assertEquals(4, list.size());

    Assert.assertEquals(2, list.get(2).getValue().getIdentity().getClusterPosition());

    list.remove(list.get(2));
    Assert.assertEquals(3, list.size());
    Assert.assertEquals(3, list.get(2).getValue().getIdentity().getClusterPosition());

    list.remove(list.get(0));
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(3, list.get(1).getValue().getIdentity().getClusterPosition());

    list.add(
        temp.createEntryInternal(new ORecordId(12, 4), OTransactionIndexChanges.OPERATION.PUT));
    Assert.assertEquals(3, list.size());
    Assert.assertEquals(4, list.get(2).getValue().getIdentity().getClusterPosition());
  }
}
