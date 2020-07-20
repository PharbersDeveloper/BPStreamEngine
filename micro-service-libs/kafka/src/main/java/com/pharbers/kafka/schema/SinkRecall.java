/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.pharbers.kafka.schema;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class SinkRecall extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 786255705856057933L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SinkRecall\",\"namespace\":\"com.pharbers.kafka.schema\",\"fields\":[{\"name\":\"jobId\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence jobId;
  @Deprecated public long count;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public SinkRecall() {}

  /**
   * All-args constructor.
   * @param jobId The new value for jobId
   * @param count The new value for count
   */
  public SinkRecall(java.lang.CharSequence jobId, java.lang.Long count) {
    this.jobId = jobId;
    this.count = count;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return jobId;
    case 1: return count;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: jobId = (java.lang.CharSequence)value$; break;
    case 1: count = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'jobId' field.
   * @return The value of the 'jobId' field.
   */
  public java.lang.CharSequence getJobId() {
    return jobId;
  }

  /**
   * Sets the value of the 'jobId' field.
   * @param value the value to set.
   */
  public void setJobId(java.lang.CharSequence value) {
    this.jobId = value;
  }

  /**
   * Gets the value of the 'count' field.
   * @return The value of the 'count' field.
   */
  public java.lang.Long getCount() {
    return count;
  }

  /**
   * Sets the value of the 'count' field.
   * @param value the value to set.
   */
  public void setCount(java.lang.Long value) {
    this.count = value;
  }

  /**
   * Creates a new SinkRecall RecordBuilder.
   * @return A new SinkRecall RecordBuilder
   */
  public static com.pharbers.kafka.schema.SinkRecall.Builder newBuilder() {
    return new com.pharbers.kafka.schema.SinkRecall.Builder();
  }

  /**
   * Creates a new SinkRecall RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new SinkRecall RecordBuilder
   */
  public static com.pharbers.kafka.schema.SinkRecall.Builder newBuilder(com.pharbers.kafka.schema.SinkRecall.Builder other) {
    return new com.pharbers.kafka.schema.SinkRecall.Builder(other);
  }

  /**
   * Creates a new SinkRecall RecordBuilder by copying an existing SinkRecall instance.
   * @param other The existing instance to copy.
   * @return A new SinkRecall RecordBuilder
   */
  public static com.pharbers.kafka.schema.SinkRecall.Builder newBuilder(com.pharbers.kafka.schema.SinkRecall other) {
    return new com.pharbers.kafka.schema.SinkRecall.Builder(other);
  }

  /**
   * RecordBuilder for SinkRecall instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<SinkRecall>
    implements org.apache.avro.data.RecordBuilder<SinkRecall> {

    private java.lang.CharSequence jobId;
    private long count;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.pharbers.kafka.schema.SinkRecall.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.jobId)) {
        this.jobId = data().deepCopy(fields()[0].schema(), other.jobId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.count)) {
        this.count = data().deepCopy(fields()[1].schema(), other.count);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing SinkRecall instance
     * @param other The existing instance to copy.
     */
    private Builder(com.pharbers.kafka.schema.SinkRecall other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.jobId)) {
        this.jobId = data().deepCopy(fields()[0].schema(), other.jobId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.count)) {
        this.count = data().deepCopy(fields()[1].schema(), other.count);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'jobId' field.
      * @return The value.
      */
    public java.lang.CharSequence getJobId() {
      return jobId;
    }

    /**
      * Sets the value of the 'jobId' field.
      * @param value The value of 'jobId'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.SinkRecall.Builder setJobId(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.jobId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'jobId' field has been set.
      * @return True if the 'jobId' field has been set, false otherwise.
      */
    public boolean hasJobId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'jobId' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.SinkRecall.Builder clearJobId() {
      jobId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'count' field.
      * @return The value.
      */
    public java.lang.Long getCount() {
      return count;
    }

    /**
      * Sets the value of the 'count' field.
      * @param value The value of 'count'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.SinkRecall.Builder setCount(long value) {
      validate(fields()[1], value);
      this.count = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'count' field has been set.
      * @return True if the 'count' field has been set, false otherwise.
      */
    public boolean hasCount() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'count' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.SinkRecall.Builder clearCount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public SinkRecall build() {
      try {
        SinkRecall record = new SinkRecall();
        record.jobId = fieldSetFlags()[0] ? this.jobId : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.count = fieldSetFlags()[1] ? this.count : (java.lang.Long) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}