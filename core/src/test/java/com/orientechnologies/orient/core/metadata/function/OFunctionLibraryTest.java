package com.orientechnologies.orient.core.metadata.function;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Test;

/** Created by tglman on 10/02/16. */
public class OFunctionLibraryTest extends BaseMemoryDatabase {

  @Test
  public void testSimpleFunctionCreate() {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNotNull(func);
  }

  @Test(expected = OFunctionDuplicatedException.class)
  public void testDuplicateFunctionCreate() {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
  }

  @Test
  public void testFunctionCreateDrop() {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNotNull(func);
    db.getMetadata().getFunctionLibrary().dropFunction("TestFunc");
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNull(func);
    func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc1");
    db.getMetadata().getFunctionLibrary().dropFunction(func);
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNull(func);
  }
}
