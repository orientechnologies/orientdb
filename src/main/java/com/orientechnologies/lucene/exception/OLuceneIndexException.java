package main.java.com.orientechnologies.lucene.exception;

/**
 * Created by enricorisa on 28/03/14.
 */
public class OLuceneIndexException extends Exception {
  public OLuceneIndexException() {
  }

  public OLuceneIndexException(String message) {
    super(message);
  }

  public OLuceneIndexException(Throwable cause) {
    super(cause);
  }

  public OLuceneIndexException(String message, Throwable cause) {
    super(message, cause);
  }
}
