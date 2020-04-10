/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.pharbers.kafka.schema;


@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class GenCubeJobSubmit extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -2353878540579659953L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GenCubeJobSubmit\",\"namespace\":\"com.pharbers.kafka.schema\",\"fields\":[{\"name\":\"InputDataType\",\"type\":\"string\"},{\"name\":\"InputPath\",\"type\":\"string\"},{\"name\":\"OutputDataType\",\"type\":\"string\"},{\"name\":\"OutputPath\",\"type\":\"string\"},{\"name\":\"Strategy\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence InputDataType;
  @Deprecated public java.lang.CharSequence InputPath;
  @Deprecated public java.lang.CharSequence OutputDataType;
  @Deprecated public java.lang.CharSequence OutputPath;
  @Deprecated public java.lang.CharSequence Strategy;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public GenCubeJobSubmit() {}

  /**
   * All-args constructor.
   * @param InputDataType The new value for InputDataType
   * @param InputPath The new value for InputPath
   * @param OutputDataType The new value for OutputDataType
   * @param OutputPath The new value for OutputPath
   * @param Strategy The new value for Strategy
   */
  public GenCubeJobSubmit(java.lang.CharSequence InputDataType, java.lang.CharSequence InputPath, java.lang.CharSequence OutputDataType, java.lang.CharSequence OutputPath, java.lang.CharSequence Strategy) {
    this.InputDataType = InputDataType;
    this.InputPath = InputPath;
    this.OutputDataType = OutputDataType;
    this.OutputPath = OutputPath;
    this.Strategy = Strategy;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return InputDataType;
    case 1: return InputPath;
    case 2: return OutputDataType;
    case 3: return OutputPath;
    case 4: return Strategy;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: InputDataType = (java.lang.CharSequence)value$; break;
    case 1: InputPath = (java.lang.CharSequence)value$; break;
    case 2: OutputDataType = (java.lang.CharSequence)value$; break;
    case 3: OutputPath = (java.lang.CharSequence)value$; break;
    case 4: Strategy = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'InputDataType' field.
   * @return The value of the 'InputDataType' field.
   */
  public java.lang.CharSequence getInputDataType() {
    return InputDataType;
  }

  /**
   * Sets the value of the 'InputDataType' field.
   * @param value the value to set.
   */
  public void setInputDataType(java.lang.CharSequence value) {
    this.InputDataType = value;
  }

  /**
   * Gets the value of the 'InputPath' field.
   * @return The value of the 'InputPath' field.
   */
  public java.lang.CharSequence getInputPath() {
    return InputPath;
  }

  /**
   * Sets the value of the 'InputPath' field.
   * @param value the value to set.
   */
  public void setInputPath(java.lang.CharSequence value) {
    this.InputPath = value;
  }

  /**
   * Gets the value of the 'OutputDataType' field.
   * @return The value of the 'OutputDataType' field.
   */
  public java.lang.CharSequence getOutputDataType() {
    return OutputDataType;
  }

  /**
   * Sets the value of the 'OutputDataType' field.
   * @param value the value to set.
   */
  public void setOutputDataType(java.lang.CharSequence value) {
    this.OutputDataType = value;
  }

  /**
   * Gets the value of the 'OutputPath' field.
   * @return The value of the 'OutputPath' field.
   */
  public java.lang.CharSequence getOutputPath() {
    return OutputPath;
  }

  /**
   * Sets the value of the 'OutputPath' field.
   * @param value the value to set.
   */
  public void setOutputPath(java.lang.CharSequence value) {
    this.OutputPath = value;
  }

  /**
   * Gets the value of the 'Strategy' field.
   * @return The value of the 'Strategy' field.
   */
  public java.lang.CharSequence getStrategy() {
    return Strategy;
  }

  /**
   * Sets the value of the 'Strategy' field.
   * @param value the value to set.
   */
  public void setStrategy(java.lang.CharSequence value) {
    this.Strategy = value;
  }

  /**
   * Creates a new GenCubeJobSubmit RecordBuilder.
   * @return A new GenCubeJobSubmit RecordBuilder
   */
  public static com.pharbers.kafka.schema.GenCubeJobSubmit.Builder newBuilder() {
    return new com.pharbers.kafka.schema.GenCubeJobSubmit.Builder();
  }

  /**
   * Creates a new GenCubeJobSubmit RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new GenCubeJobSubmit RecordBuilder
   */
  public static com.pharbers.kafka.schema.GenCubeJobSubmit.Builder newBuilder(com.pharbers.kafka.schema.GenCubeJobSubmit.Builder other) {
    return new com.pharbers.kafka.schema.GenCubeJobSubmit.Builder(other);
  }

  /**
   * Creates a new GenCubeJobSubmit RecordBuilder by copying an existing GenCubeJobSubmit instance.
   * @param other The existing instance to copy.
   * @return A new GenCubeJobSubmit RecordBuilder
   */
  public static com.pharbers.kafka.schema.GenCubeJobSubmit.Builder newBuilder(com.pharbers.kafka.schema.GenCubeJobSubmit other) {
    return new com.pharbers.kafka.schema.GenCubeJobSubmit.Builder(other);
  }

  /**
   * RecordBuilder for GenCubeJobSubmit instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<GenCubeJobSubmit>
    implements org.apache.avro.data.RecordBuilder<GenCubeJobSubmit> {

    private java.lang.CharSequence InputDataType;
    private java.lang.CharSequence InputPath;
    private java.lang.CharSequence OutputDataType;
    private java.lang.CharSequence OutputPath;
    private java.lang.CharSequence Strategy;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.pharbers.kafka.schema.GenCubeJobSubmit.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.InputDataType)) {
        this.InputDataType = data().deepCopy(fields()[0].schema(), other.InputDataType);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.InputPath)) {
        this.InputPath = data().deepCopy(fields()[1].schema(), other.InputPath);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.OutputDataType)) {
        this.OutputDataType = data().deepCopy(fields()[2].schema(), other.OutputDataType);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.OutputPath)) {
        this.OutputPath = data().deepCopy(fields()[3].schema(), other.OutputPath);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.Strategy)) {
        this.Strategy = data().deepCopy(fields()[4].schema(), other.Strategy);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing GenCubeJobSubmit instance
     * @param other The existing instance to copy.
     */
    private Builder(com.pharbers.kafka.schema.GenCubeJobSubmit other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.InputDataType)) {
        this.InputDataType = data().deepCopy(fields()[0].schema(), other.InputDataType);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.InputPath)) {
        this.InputPath = data().deepCopy(fields()[1].schema(), other.InputPath);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.OutputDataType)) {
        this.OutputDataType = data().deepCopy(fields()[2].schema(), other.OutputDataType);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.OutputPath)) {
        this.OutputPath = data().deepCopy(fields()[3].schema(), other.OutputPath);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.Strategy)) {
        this.Strategy = data().deepCopy(fields()[4].schema(), other.Strategy);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'InputDataType' field.
      * @return The value.
      */
    public java.lang.CharSequence getInputDataType() {
      return InputDataType;
    }

    /**
      * Sets the value of the 'InputDataType' field.
      * @param value The value of 'InputDataType'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder setInputDataType(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.InputDataType = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'InputDataType' field has been set.
      * @return True if the 'InputDataType' field has been set, false otherwise.
      */
    public boolean hasInputDataType() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'InputDataType' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder clearInputDataType() {
      InputDataType = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'InputPath' field.
      * @return The value.
      */
    public java.lang.CharSequence getInputPath() {
      return InputPath;
    }

    /**
      * Sets the value of the 'InputPath' field.
      * @param value The value of 'InputPath'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder setInputPath(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.InputPath = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'InputPath' field has been set.
      * @return True if the 'InputPath' field has been set, false otherwise.
      */
    public boolean hasInputPath() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'InputPath' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder clearInputPath() {
      InputPath = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'OutputDataType' field.
      * @return The value.
      */
    public java.lang.CharSequence getOutputDataType() {
      return OutputDataType;
    }

    /**
      * Sets the value of the 'OutputDataType' field.
      * @param value The value of 'OutputDataType'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder setOutputDataType(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.OutputDataType = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'OutputDataType' field has been set.
      * @return True if the 'OutputDataType' field has been set, false otherwise.
      */
    public boolean hasOutputDataType() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'OutputDataType' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder clearOutputDataType() {
      OutputDataType = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'OutputPath' field.
      * @return The value.
      */
    public java.lang.CharSequence getOutputPath() {
      return OutputPath;
    }

    /**
      * Sets the value of the 'OutputPath' field.
      * @param value The value of 'OutputPath'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder setOutputPath(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.OutputPath = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'OutputPath' field has been set.
      * @return True if the 'OutputPath' field has been set, false otherwise.
      */
    public boolean hasOutputPath() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'OutputPath' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder clearOutputPath() {
      OutputPath = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'Strategy' field.
      * @return The value.
      */
    public java.lang.CharSequence getStrategy() {
      return Strategy;
    }

    /**
      * Sets the value of the 'Strategy' field.
      * @param value The value of 'Strategy'.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder setStrategy(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.Strategy = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'Strategy' field has been set.
      * @return True if the 'Strategy' field has been set, false otherwise.
      */
    public boolean hasStrategy() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'Strategy' field.
      * @return This builder.
      */
    public com.pharbers.kafka.schema.GenCubeJobSubmit.Builder clearStrategy() {
      Strategy = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public GenCubeJobSubmit build() {
      try {
        GenCubeJobSubmit record = new GenCubeJobSubmit();
        record.InputDataType = fieldSetFlags()[0] ? this.InputDataType : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.InputPath = fieldSetFlags()[1] ? this.InputPath : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.OutputDataType = fieldSetFlags()[2] ? this.OutputDataType : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.OutputPath = fieldSetFlags()[3] ? this.OutputPath : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.Strategy = fieldSetFlags()[4] ? this.Strategy : (java.lang.CharSequence) defaultValue(fields()[4]);
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