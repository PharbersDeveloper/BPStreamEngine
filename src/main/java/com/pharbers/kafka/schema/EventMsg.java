/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.pharbers.kafka.schema;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class EventMsg extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -4690517135389631736L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"EventMsg\",\"namespace\":\"com.pharbers.kafka.schema\",\"fields\":[{\"name\":\"jobId\",\"type\":\"string\"},{\"name\":\"traceId\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"data\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence jobId;
  @Deprecated public java.lang.CharSequence traceId;
  @Deprecated public java.lang.CharSequence type;
  @Deprecated public java.lang.CharSequence data;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public EventMsg() {}

  /**
   * All-args constructor.
   * @param jobId The new value for jobId
   * @param traceId The new value for traceId
   * @param type The new value for type
   * @param data The new value for data
   */
  public EventMsg(java.lang.CharSequence jobId, java.lang.CharSequence traceId, java.lang.CharSequence type, java.lang.CharSequence data) {
    this.jobId = jobId;
    this.traceId = traceId;
    this.type = type;
    this.data = data;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return jobId;
    case 1: return traceId;
    case 2: return type;
    case 3: return data;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: jobId = (java.lang.CharSequence)value$; break;
    case 1: traceId = (java.lang.CharSequence)value$; break;
    case 2: type = (java.lang.CharSequence)value$; break;
    case 3: data = (java.lang.CharSequence)value$; break;
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
   * Gets the value of the 'traceId' field.
   * @return The value of the 'traceId' field.
   */
  public java.lang.CharSequence getTraceId() {
    return traceId;
  }

  /**
   * Sets the value of the 'traceId' field.
   * @param value the value to set.
   */
  public void setTraceId(java.lang.CharSequence value) {
    this.traceId = value;
  }

  /**
   * Gets the value of the 'type' field.
   * @return The value of the 'type' field.
   */
  public java.lang.CharSequence getType() {
    return type;
  }

  /**
   * Sets the value of the 'type' field.
   * @param value the value to set.
   */
  public void setType(java.lang.CharSequence value) {
    this.type = value;
  }

  /**
   * Gets the value of the 'data' field.
   * @return The value of the 'data' field.
   */
  public java.lang.CharSequence getData() {
    return data;
  }

  /**
   * Sets the value of the 'data' field.
   * @param value the value to set.
   */
  public void setData(java.lang.CharSequence value) {
    this.data = value;
  }

  /**
   * Creates a new EventMsg RecordBuilder.
   * @return A new EventMsg RecordBuilder
   */
  public static com.pharbers.kafka.schema.EventMsg.Builder newBuilder() {
    return new com.pharbers.kafka.schema.EventMsg.Builder();
  }

  /**
   * Creates a new EventMsg RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new EventMsg RecordBuilder
   */
  public static com.pharbers.kafka.schema.EventMsg.Builder newBuilder(com.pharbers.kafka.schema.EventMsg.Builder other) {
    return new com.pharbers.kafka.schema.EventMsg.Builder(other);
  }

  /**
   * Creates a new EventMsg RecordBuilder by copying an existing EventMsg instance.
   * @param other The existing instance to copy.
   * @return A new EventMsg RecordBuilder
   */
  public static com.pharbers.kafka.schema.EventMsg.Builder newBuilder(com.pharbers.kafka.schema.EventMsg other) {
    return new com.pharbers.kafka.schema.EventMsg.Builder(other);
  }

  /**
   * RecordBuilder for EventMsg instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<EventMsg>
    implements org.apache.avro.data.RecordBuilder<EventMsg> {

    private java.lang.CharSequence jobId;
    private java.lang.CharSequence traceId;
    private java.lang.CharSequence type;
    private java.lang.CharSequence data;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.pharbers.kafka.schema.EventMsg.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.jobId)) {
        this.jobId = data().deepCopy(fields()[0].schema(), other.jobId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.traceId)) {
        this.traceId = data().deepCopy(fields()[1].schema(), other.traceId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.type)) {
        this.type = data().deepCopy(fields()[2].schema(), other.type);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.data)) {
        this.data = data().deepCopy(fields()[3].schema(), other.data);
        fieldSetFlags()[3] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing EventMsg instance
     * @param other The existing instance to copy.
     */
    private Builder(com.pharbers.kafka.schema.EventMsg other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.jobId)) {
        this.jobId = data().deepCopy(fields()[0].schema(), other.jobId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.traceId)) {
        this.traceId = data().deepCopy(fields()[1].schema(), other.traceId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.type)) {
        this.type = data().deepCopy(fields()[2].schema(), other.type);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.data)) {
        this.data = data().deepCopy(fields()[3].schema(), other.data);
        fieldSetFlags()[3] = true;
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
    public com.pharbers.kafka.schema.EventMsg.Builder setJobId(java.lang.CharSequence value) {
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
    public com.pharbers.kafka.schema.EventMsg.Builder clearJobId() {
      jobId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'traceId' field.
      * @return The value.
      */
    public java.lang.CharSequence getTraceId() {
      return traceId;
    }

    /**
      * Sets the value of the 'traceId' field.
      * @param value The value of 'traceId'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.EventMsg.Builder setTraceId(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.traceId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'traceId' field has been set.
      * @return True if the 'traceId' field has been set, false otherwise.
      */
    public boolean hasTraceId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'traceId' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.EventMsg.Builder clearTraceId() {
      traceId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'type' field.
      * @return The value.
      */
    public java.lang.CharSequence getType() {
      return type;
    }

    /**
      * Sets the value of the 'type' field.
      * @param value The value of 'type'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.EventMsg.Builder setType(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.type = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'type' field has been set.
      * @return True if the 'type' field has been set, false otherwise.
      */
    public boolean hasType() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'type' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.EventMsg.Builder clearType() {
      type = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'data' field.
      * @return The value.
      */
    public java.lang.CharSequence getData() {
      return data;
    }

    /**
      * Sets the value of the 'data' field.
      * @param value The value of 'data'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.EventMsg.Builder setData(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.data = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'data' field has been set.
      * @return True if the 'data' field has been set, false otherwise.
      */
    public boolean hasData() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'data' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.EventMsg.Builder clearData() {
      data = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    public EventMsg build() {
      try {
        EventMsg record = new EventMsg();
        record.jobId = fieldSetFlags()[0] ? this.jobId : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.traceId = fieldSetFlags()[1] ? this.traceId : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.type = fieldSetFlags()[2] ? this.type : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.data = fieldSetFlags()[3] ? this.data : (java.lang.CharSequence) defaultValue(fields()[3]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);


  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);


}