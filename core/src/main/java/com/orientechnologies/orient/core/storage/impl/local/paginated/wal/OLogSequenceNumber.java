package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public class OLogSequenceNumber implements Comparable<OLogSequenceNumber> {
  public static final OLogSequenceNumber BEGIN = new OLogSequenceNumber(0, 0);

  private final int                      segment;
  private final long                     position;

  public OLogSequenceNumber(int segment, long position) {
    assert segment >= 0;
    assert position >= 0;

    this.segment = segment;
    this.position = position;
  }

  public int getSegment() {
    return segment;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OLogSequenceNumber that = (OLogSequenceNumber) o;

    if (position != that.position)
      return false;
    if (segment != that.segment)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = segment;
    result = 31 * result + (int) (position ^ (position >>> 32));
    return result;
  }

  @Override
  public int compareTo(OLogSequenceNumber otherNumber) {
    if (segment > otherNumber.segment)
      return 1;
    if (segment < otherNumber.segment)
      return -1;

    if (position > otherNumber.position)
      return 1;
    else if (position < otherNumber.position)
      return -1;

    return 0;
  }

  @Override
  public String toString() {
    return "OLogSequenceNumber{" + "segment=" + segment + ", position=" + position + '}';
  }
}
