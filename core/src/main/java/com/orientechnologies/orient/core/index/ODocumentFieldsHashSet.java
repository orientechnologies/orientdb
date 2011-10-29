package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;

public class ODocumentFieldsHashSet extends AbstractSet<ODocument> implements Serializable {
  private final HashSet<ODocumentWrapper> hashSet;


  public ODocumentFieldsHashSet() {
    hashSet = new HashSet<ODocumentWrapper>();
  }

  @Override
  public boolean contains(Object o) {
    if(!(o instanceof ODocument))
      return false;

    return hashSet.contains(new ODocumentWrapper((ODocument)o));
  }

  @Override
  public boolean remove(Object o) {
    if(!(o instanceof ODocument))
      return false;

    return hashSet.remove(new ODocumentWrapper((ODocument)o));
  }

  @Override
  public boolean add(ODocument document) {
    return hashSet.add(new ODocumentWrapper(document));
  }

  @Override
  public boolean isEmpty() {
    return hashSet.isEmpty();
  }

  @Override
  public void clear() {
    hashSet.clear();
  }

  @Override
  public Iterator<ODocument> iterator() {
    final Iterator<ODocumentWrapper> iterator = hashSet.iterator();
    return new Iterator<ODocument>() {
      public boolean hasNext() {
        return iterator.hasNext();
      }

      public ODocument next() {
        return iterator.next().document;
      }

      public void remove() {
        iterator.remove();
      }
    };
  }

  @Override
  public int size() {
    return hashSet.size();
  }

  private static final class ODocumentWrapper {
    private final ODocument document;

    private ODocumentWrapper(ODocument document) {
      this.document = document;
    }

    @Override
    public int hashCode() {
      int hashCode = document.getIdentity().hashCode();

      for(Object field : document.fieldValues())
         hashCode = 31*hashCode + field.hashCode();

      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if(obj == null)
        return false;

      if(obj.getClass() != document.getClass())
        return false;

      if(obj == document)
        return true;

      final ODocument anotherDocument = (ODocument) obj;

      if(!document.getIdentity().equals(anotherDocument))
        return false;

      final String[] filedNames = document.fieldNames();
      final String[] anotherFieldNames = anotherDocument.fieldNames();

      if(filedNames.length != anotherFieldNames.length)
        return false;

      for(final String fieldName : filedNames) {
        final Object fieldValue = document.field(fieldName);
        final Object anotherFieldValue = anotherDocument.field(fieldName);

        if(fieldValue == null && anotherFieldValue != null)
          return false;

        if(fieldValue != null && !fieldValue.equals(anotherFieldValue))
          return false;
      }

      return true;
    }

    @Override
    public String toString() {
      return document.toString();
    }
  }
}
