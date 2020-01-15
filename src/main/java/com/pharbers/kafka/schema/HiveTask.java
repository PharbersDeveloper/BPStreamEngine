/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.pharbers.kafka.schema;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class HiveTask extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6204888610544969929L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"HiveTask\",\"namespace\":\"com.pharbers.kafka.schema\",\"fields\":[{\"name\":\"jobId\",\"type\":\"string\"},{\"name\":\"traceId\",\"type\":\"string\"},{\"name\":\"datasetId\",\"type\":\"string\"},{\"name\":\"taskType\",\"type\":\"string\"},{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"length\",\"type\":\"int\"},{\"name\":\"remarks\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence jobId;
  @Deprecated public java.lang.CharSequence traceId;
  @Deprecated public java.lang.CharSequence datasetId;
  @Deprecated public java.lang.CharSequence taskType;
  @Deprecated public java.lang.CharSequence url;
  @Deprecated public int length;
  @Deprecated public java.lang.CharSequence remarks;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public HiveTask() {}

  /**
   * All-args constructor.
   * @param jobId The new value for jobId
   * @param traceId The new value for traceId
   * @param datasetId The new value for datasetId
   * @param taskType The new value for taskType
   * @param url The new value for url
   * @param length The new value for length
   * @param remarks The new value for remarks
   */
  public HiveTask(java.lang.CharSequence jobId, java.lang.CharSequence traceId, java.lang.CharSequence datasetId, java.lang.CharSequence taskType, java.lang.CharSequence url, java.lang.Integer length, java.lang.CharSequence remarks) {
    this.jobId = jobId;
    this.traceId = traceId;
    this.datasetId = datasetId;
    this.taskType = taskType;
    this.url = url;
    this.length = length;
    this.remarks = remarks;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return jobId;
    case 1: return traceId;
    case 2: return datasetId;
    case 3: return taskType;
    case 4: return url;
    case 5: return length;
    case 6: return remarks;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: jobId = (java.lang.CharSequence)value$; break;
    case 1: traceId = (java.lang.CharSequence)value$; break;
    case 2: datasetId = (java.lang.CharSequence)value$; break;
    case 3: taskType = (java.lang.CharSequence)value$; break;
    case 4: url = (java.lang.CharSequence)value$; break;
    case 5: length = (java.lang.Integer)value$; break;
    case 6: remarks = (java.lang.CharSequence)value$; break;
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
   * Gets the value of the 'datasetId' field.
   * @return The value of the 'datasetId' field.
   */
  public java.lang.CharSequence getDatasetId() {
    return datasetId;
  }

  /**
   * Sets the value of the 'datasetId' field.
   * @param value the value to set.
   */
  public void setDatasetId(java.lang.CharSequence value) {
    this.datasetId = value;
  }

  /**
   * Gets the value of the 'taskType' field.
   * @return The value of the 'taskType' field.
   */
  public java.lang.CharSequence getTaskType() {
    return taskType;
  }

  /**
   * Sets the value of the 'taskType' field.
   * @param value the value to set.
   */
  public void setTaskType(java.lang.CharSequence value) {
    this.taskType = value;
  }

  /**
   * Gets the value of the 'url' field.
   * @return The value of the 'url' field.
   */
  public java.lang.CharSequence getUrl() {
    return url;
  }

  /**
   * Sets the value of the 'url' field.
   * @param value the value to set.
   */
  public void setUrl(java.lang.CharSequence value) {
    this.url = value;
  }

  /**
   * Gets the value of the 'length' field.
   * @return The value of the 'length' field.
   */
  public java.lang.Integer getLength() {
    return length;
  }

  /**
   * Sets the value of the 'length' field.
   * @param value the value to set.
   */
  public void setLength(java.lang.Integer value) {
    this.length = value;
  }

  /**
   * Gets the value of the 'remarks' field.
   * @return The value of the 'remarks' field.
   */
  public java.lang.CharSequence getRemarks() {
    return remarks;
  }

  /**
   * Sets the value of the 'remarks' field.
   * @param value the value to set.
   */
  public void setRemarks(java.lang.CharSequence value) {
    this.remarks = value;
  }

  /**
   * Creates a new HiveTask RecordBuilder.
   * @return A new HiveTask RecordBuilder
   */
  public static com.pharbers.kafka.schema.HiveTask.Builder newBuilder() {
    return new com.pharbers.kafka.schema.HiveTask.Builder();
  }

  /**
   * Creates a new HiveTask RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new HiveTask RecordBuilder
   */
  public static com.pharbers.kafka.schema.HiveTask.Builder newBuilder(com.pharbers.kafka.schema.HiveTask.Builder other) {
    return new com.pharbers.kafka.schema.HiveTask.Builder(other);
  }

  /**
   * Creates a new HiveTask RecordBuilder by copying an existing HiveTask instance.
   * @param other The existing instance to copy.
   * @return A new HiveTask RecordBuilder
   */
  public static com.pharbers.kafka.schema.HiveTask.Builder newBuilder(com.pharbers.kafka.schema.HiveTask other) {
    return new com.pharbers.kafka.schema.HiveTask.Builder(other);
  }

  /**
   * RecordBuilder for HiveTask instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<HiveTask>
    implements org.apache.avro.data.RecordBuilder<HiveTask> {

    private java.lang.CharSequence jobId;
    private java.lang.CharSequence traceId;
    private java.lang.CharSequence datasetId;
    private java.lang.CharSequence taskType;
    private java.lang.CharSequence url;
    private int length;
    private java.lang.CharSequence remarks;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.pharbers.kafka.schema.HiveTask.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.jobId)) {
        this.jobId = data().deepCopy(fields()[0].schema(), other.jobId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.traceId)) {
        this.traceId = data().deepCopy(fields()[1].schema(), other.traceId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.datasetId)) {
        this.datasetId = data().deepCopy(fields()[2].schema(), other.datasetId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.taskType)) {
        this.taskType = data().deepCopy(fields()[3].schema(), other.taskType);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.url)) {
        this.url = data().deepCopy(fields()[4].schema(), other.url);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.length)) {
        this.length = data().deepCopy(fields()[5].schema(), other.length);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.remarks)) {
        this.remarks = data().deepCopy(fields()[6].schema(), other.remarks);
        fieldSetFlags()[6] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing HiveTask instance
     * @param other The existing instance to copy.
     */
    private Builder(com.pharbers.kafka.schema.HiveTask other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.jobId)) {
        this.jobId = data().deepCopy(fields()[0].schema(), other.jobId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.traceId)) {
        this.traceId = data().deepCopy(fields()[1].schema(), other.traceId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.datasetId)) {
        this.datasetId = data().deepCopy(fields()[2].schema(), other.datasetId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.taskType)) {
        this.taskType = data().deepCopy(fields()[3].schema(), other.taskType);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.url)) {
        this.url = data().deepCopy(fields()[4].schema(), other.url);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.length)) {
        this.length = data().deepCopy(fields()[5].schema(), other.length);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.remarks)) {
        this.remarks = data().deepCopy(fields()[6].schema(), other.remarks);
        fieldSetFlags()[6] = true;
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
    public com.pharbers.kafka.schema.HiveTask.Builder setJobId(java.lang.CharSequence value) {
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
    public com.pharbers.kafka.schema.HiveTask.Builder clearJobId() {
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
    public com.pharbers.kafka.schema.HiveTask.Builder setTraceId(java.lang.CharSequence value) {
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
    public com.pharbers.kafka.schema.HiveTask.Builder clearTraceId() {
      traceId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'datasetId' field.
      * @return The value.
      */
    public java.lang.CharSequence getDatasetId() {
      return datasetId;
    }

    /**
      * Sets the value of the 'datasetId' field.
      * @param value The value of 'datasetId'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder setDatasetId(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.datasetId = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'datasetId' field has been set.
      * @return True if the 'datasetId' field has been set, false otherwise.
      */
    public boolean hasDatasetId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'datasetId' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder clearDatasetId() {
      datasetId = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'taskType' field.
      * @return The value.
      */
    public java.lang.CharSequence getTaskType() {
      return taskType;
    }

    /**
      * Sets the value of the 'taskType' field.
      * @param value The value of 'taskType'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder setTaskType(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.taskType = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'taskType' field has been set.
      * @return True if the 'taskType' field has been set, false otherwise.
      */
    public boolean hasTaskType() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'taskType' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder clearTaskType() {
      taskType = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'url' field.
      * @return The value.
      */
    public java.lang.CharSequence getUrl() {
      return url;
    }

    /**
      * Sets the value of the 'url' field.
      * @param value The value of 'url'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder setUrl(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.url = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'url' field has been set.
      * @return True if the 'url' field has been set, false otherwise.
      */
    public boolean hasUrl() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'url' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder clearUrl() {
      url = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'length' field.
      * @return The value.
      */
    public java.lang.Integer getLength() {
      return length;
    }

    /**
      * Sets the value of the 'length' field.
      * @param value The value of 'length'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder setLength(int value) {
      validate(fields()[5], value);
      this.length = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'length' field has been set.
      * @return True if the 'length' field has been set, false otherwise.
      */
    public boolean hasLength() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'length' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder clearLength() {
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'remarks' field.
      * @return The value.
      */
    public java.lang.CharSequence getRemarks() {
      return remarks;
    }

    /**
      * Sets the value of the 'remarks' field.
      * @param value The value of 'remarks'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder setRemarks(java.lang.CharSequence value) {
      validate(fields()[6], value);
      this.remarks = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'remarks' field has been set.
      * @return True if the 'remarks' field has been set, false otherwise.
      */
    public boolean hasRemarks() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'remarks' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.HiveTask.Builder clearRemarks() {
      remarks = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    @Override
    public HiveTask build() {
      try {
        HiveTask record = new HiveTask();
        record.jobId = fieldSetFlags()[0] ? this.jobId : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.traceId = fieldSetFlags()[1] ? this.traceId : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.datasetId = fieldSetFlags()[2] ? this.datasetId : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.taskType = fieldSetFlags()[3] ? this.taskType : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.url = fieldSetFlags()[4] ? this.url : (java.lang.CharSequence) defaultValue(fields()[4]);
        record.length = fieldSetFlags()[5] ? this.length : (java.lang.Integer) defaultValue(fields()[5]);
        record.remarks = fieldSetFlags()[6] ? this.remarks : (java.lang.CharSequence) defaultValue(fields()[6]);
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
