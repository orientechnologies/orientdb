package com.orientechnologies.orient.core.sql.parser;

/**
 * This class is used by modifiers to return the documents that have to be updated AND the field that has to be updated.
 * Further operatios are then performed by the top level UpdateItem
 * <p>
 * Eg.
 * <p>
 * UPDATE Foo SET foo.bar.baz.name = 'xx'
 * <p>
 * the chain is following:
 * <p>
 * (identifier: foo) -> (modifier: bar) -> (modifier: baz) -> (modifier: name)
 * <p>
 * The top level UpdateItem calculates the value foo and will pass it to the modifier.
 * The modifier calculats the value of &ltdocsToUpdate&gt; = foo.bar.baz (that is a collection) an returns
 * to the top level UpdateItem an UpdateContext containig { docsToUpdate = &ltdocsToUpdate&gt;, fieldToSet = 'name'}
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UpdateContext {
  public Iterable  docsToUpdate;
  public OIdentifier fieldToSet;
}
