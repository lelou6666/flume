/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.cloudera.flume.handlers.thrift;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftFlumeEvent implements org.apache.thrift.TBase<ThriftFlumeEvent, ThriftFlumeEvent._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ThriftFlumeEvent");

  private static final org.apache.thrift.protocol.TField TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("timestamp", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField PRIORITY_FIELD_DESC = new org.apache.thrift.protocol.TField("priority", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField BODY_FIELD_DESC = new org.apache.thrift.protocol.TField("body", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField NANOS_FIELD_DESC = new org.apache.thrift.protocol.TField("nanos", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField HOST_FIELD_DESC = new org.apache.thrift.protocol.TField("host", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField FIELDS_FIELD_DESC = new org.apache.thrift.protocol.TField("fields", org.apache.thrift.protocol.TType.MAP, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ThriftFlumeEventStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ThriftFlumeEventTupleSchemeFactory());
  }

  public long timestamp; // required
  /**
   * 
   * @see Priority
   */
  public Priority priority; // required
  public ByteBuffer body; // required
  public long nanos; // required
  public String host; // required
  public Map<String,ByteBuffer> fields; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIMESTAMP((short)1, "timestamp"),
    /**
     * 
     * @see Priority
     */
    PRIORITY((short)2, "priority"),
    BODY((short)3, "body"),
    NANOS((short)4, "nanos"),
    HOST((short)5, "host"),
    FIELDS((short)6, "fields");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TIMESTAMP
          return TIMESTAMP;
        case 2: // PRIORITY
          return PRIORITY;
        case 3: // BODY
          return BODY;
        case 4: // NANOS
          return NANOS;
        case 5: // HOST
          return HOST;
        case 6: // FIELDS
          return FIELDS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __TIMESTAMP_ISSET_ID = 0;
  private static final int __NANOS_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("timestamp", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "Timestamp")));
    tmpMap.put(_Fields.PRIORITY, new org.apache.thrift.meta_data.FieldMetaData("priority", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, Priority.class)));
    tmpMap.put(_Fields.BODY, new org.apache.thrift.meta_data.FieldMetaData("body", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.NANOS, new org.apache.thrift.meta_data.FieldMetaData("nanos", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.HOST, new org.apache.thrift.meta_data.FieldMetaData("host", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.FIELDS, new org.apache.thrift.meta_data.FieldMetaData("fields", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ThriftFlumeEvent.class, metaDataMap);
  }

  public ThriftFlumeEvent() {
  }

  public ThriftFlumeEvent(
    long timestamp,
    Priority priority,
    ByteBuffer body,
    long nanos,
    String host,
    Map<String,ByteBuffer> fields)
  {
    this();
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    this.priority = priority;
    this.body = body;
    this.nanos = nanos;
    setNanosIsSet(true);
    this.host = host;
    this.fields = fields;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThriftFlumeEvent(ThriftFlumeEvent other) {
    __isset_bitfield = other.__isset_bitfield;
    this.timestamp = other.timestamp;
    if (other.isSetPriority()) {
      this.priority = other.priority;
    }
    if (other.isSetBody()) {
      this.body = org.apache.thrift.TBaseHelper.copyBinary(other.body);
;
    }
    this.nanos = other.nanos;
    if (other.isSetHost()) {
      this.host = other.host;
    }
    if (other.isSetFields()) {
      Map<String,ByteBuffer> __this__fields = new HashMap<String,ByteBuffer>();
      for (Map.Entry<String, ByteBuffer> other_element : other.fields.entrySet()) {

        String other_element_key = other_element.getKey();
        ByteBuffer other_element_value = other_element.getValue();

        String __this__fields_copy_key = other_element_key;

        ByteBuffer __this__fields_copy_value = org.apache.thrift.TBaseHelper.copyBinary(other_element_value);
;

        __this__fields.put(__this__fields_copy_key, __this__fields_copy_value);
      }
      this.fields = __this__fields;
    }
  }

  public ThriftFlumeEvent deepCopy() {
    return new ThriftFlumeEvent(this);
  }

  @Override
  public void clear() {
    setTimestampIsSet(false);
    this.timestamp = 0;
    this.priority = null;
    this.body = null;
    setNanosIsSet(false);
    this.nanos = 0;
    this.host = null;
    this.fields = null;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public ThriftFlumeEvent setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    return this;
  }

  public void unsetTimestamp() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field timestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetTimestamp() {
    return EncodingUtils.testBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
  }

  public void setTimestampIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TIMESTAMP_ISSET_ID, value);
  }

  /**
   * 
   * @see Priority
   */
  public Priority getPriority() {
    return this.priority;
  }

  /**
   * 
   * @see Priority
   */
  public ThriftFlumeEvent setPriority(Priority priority) {
    this.priority = priority;
    return this;
  }

  public void unsetPriority() {
    this.priority = null;
  }

  /** Returns true if field priority is set (has been assigned a value) and false otherwise */
  public boolean isSetPriority() {
    return this.priority != null;
  }

  public void setPriorityIsSet(boolean value) {
    if (!value) {
      this.priority = null;
    }
  }

  public byte[] getBody() {
    setBody(org.apache.thrift.TBaseHelper.rightSize(body));
    return body == null ? null : body.array();
  }

  public ByteBuffer bufferForBody() {
    return body;
  }

  public ThriftFlumeEvent setBody(byte[] body) {
    setBody(body == null ? (ByteBuffer)null : ByteBuffer.wrap(body));
    return this;
  }

  public ThriftFlumeEvent setBody(ByteBuffer body) {
    this.body = body;
    return this;
  }

  public void unsetBody() {
    this.body = null;
  }

  /** Returns true if field body is set (has been assigned a value) and false otherwise */
  public boolean isSetBody() {
    return this.body != null;
  }

  public void setBodyIsSet(boolean value) {
    if (!value) {
      this.body = null;
    }
  }

  public long getNanos() {
    return this.nanos;
  }

  public ThriftFlumeEvent setNanos(long nanos) {
    this.nanos = nanos;
    setNanosIsSet(true);
    return this;
  }

  public void unsetNanos() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NANOS_ISSET_ID);
  }

  /** Returns true if field nanos is set (has been assigned a value) and false otherwise */
  public boolean isSetNanos() {
    return EncodingUtils.testBit(__isset_bitfield, __NANOS_ISSET_ID);
  }

  public void setNanosIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NANOS_ISSET_ID, value);
  }

  public String getHost() {
    return this.host;
  }

  public ThriftFlumeEvent setHost(String host) {
    this.host = host;
    return this;
  }

  public void unsetHost() {
    this.host = null;
  }

  /** Returns true if field host is set (has been assigned a value) and false otherwise */
  public boolean isSetHost() {
    return this.host != null;
  }

  public void setHostIsSet(boolean value) {
    if (!value) {
      this.host = null;
    }
  }

  public int getFieldsSize() {
    return (this.fields == null) ? 0 : this.fields.size();
  }

  public void putToFields(String key, ByteBuffer val) {
    if (this.fields == null) {
      this.fields = new HashMap<String,ByteBuffer>();
    }
    this.fields.put(key, val);
  }

  public Map<String,ByteBuffer> getFields() {
    return this.fields;
  }

  public ThriftFlumeEvent setFields(Map<String,ByteBuffer> fields) {
    this.fields = fields;
    return this;
  }

  public void unsetFields() {
    this.fields = null;
  }

  /** Returns true if field fields is set (has been assigned a value) and false otherwise */
  public boolean isSetFields() {
    return this.fields != null;
  }

  public void setFieldsIsSet(boolean value) {
    if (!value) {
      this.fields = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TIMESTAMP:
      if (value == null) {
        unsetTimestamp();
      } else {
        setTimestamp((Long)value);
      }
      break;

    case PRIORITY:
      if (value == null) {
        unsetPriority();
      } else {
        setPriority((Priority)value);
      }
      break;

    case BODY:
      if (value == null) {
        unsetBody();
      } else {
        setBody((ByteBuffer)value);
      }
      break;

    case NANOS:
      if (value == null) {
        unsetNanos();
      } else {
        setNanos((Long)value);
      }
      break;

    case HOST:
      if (value == null) {
        unsetHost();
      } else {
        setHost((String)value);
      }
      break;

    case FIELDS:
      if (value == null) {
        unsetFields();
      } else {
        setFields((Map<String,ByteBuffer>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TIMESTAMP:
      return Long.valueOf(getTimestamp());

    case PRIORITY:
      return getPriority();

    case BODY:
      return getBody();

    case NANOS:
      return Long.valueOf(getNanos());

    case HOST:
      return getHost();

    case FIELDS:
      return getFields();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TIMESTAMP:
      return isSetTimestamp();
    case PRIORITY:
      return isSetPriority();
    case BODY:
      return isSetBody();
    case NANOS:
      return isSetNanos();
    case HOST:
      return isSetHost();
    case FIELDS:
      return isSetFields();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ThriftFlumeEvent)
      return this.equals((ThriftFlumeEvent)that);
    return false;
  }

  public boolean equals(ThriftFlumeEvent that) {
    if (that == null)
      return false;

    boolean this_present_timestamp = true;
    boolean that_present_timestamp = true;
    if (this_present_timestamp || that_present_timestamp) {
      if (!(this_present_timestamp && that_present_timestamp))
        return false;
      if (this.timestamp != that.timestamp)
        return false;
    }

    boolean this_present_priority = true && this.isSetPriority();
    boolean that_present_priority = true && that.isSetPriority();
    if (this_present_priority || that_present_priority) {
      if (!(this_present_priority && that_present_priority))
        return false;
      if (!this.priority.equals(that.priority))
        return false;
    }

    boolean this_present_body = true && this.isSetBody();
    boolean that_present_body = true && that.isSetBody();
    if (this_present_body || that_present_body) {
      if (!(this_present_body && that_present_body))
        return false;
      if (!this.body.equals(that.body))
        return false;
    }

    boolean this_present_nanos = true;
    boolean that_present_nanos = true;
    if (this_present_nanos || that_present_nanos) {
      if (!(this_present_nanos && that_present_nanos))
        return false;
      if (this.nanos != that.nanos)
        return false;
    }

    boolean this_present_host = true && this.isSetHost();
    boolean that_present_host = true && that.isSetHost();
    if (this_present_host || that_present_host) {
      if (!(this_present_host && that_present_host))
        return false;
      if (!this.host.equals(that.host))
        return false;
    }

    boolean this_present_fields = true && this.isSetFields();
    boolean that_present_fields = true && that.isSetFields();
    if (this_present_fields || that_present_fields) {
      if (!(this_present_fields && that_present_fields))
        return false;
      if (!this.fields.equals(that.fields))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_timestamp = true;
    builder.append(present_timestamp);
    if (present_timestamp)
      builder.append(timestamp);

    boolean present_priority = true && (isSetPriority());
    builder.append(present_priority);
    if (present_priority)
      builder.append(priority.getValue());

    boolean present_body = true && (isSetBody());
    builder.append(present_body);
    if (present_body)
      builder.append(body);

    boolean present_nanos = true;
    builder.append(present_nanos);
    if (present_nanos)
      builder.append(nanos);

    boolean present_host = true && (isSetHost());
    builder.append(present_host);
    if (present_host)
      builder.append(host);

    boolean present_fields = true && (isSetFields());
    builder.append(present_fields);
    if (present_fields)
      builder.append(fields);

    return builder.toHashCode();
  }

  public int compareTo(ThriftFlumeEvent other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ThriftFlumeEvent typedOther = (ThriftFlumeEvent)other;

    lastComparison = Boolean.valueOf(isSetTimestamp()).compareTo(typedOther.isSetTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTimestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.timestamp, typedOther.timestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPriority()).compareTo(typedOther.isSetPriority());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPriority()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.priority, typedOther.priority);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBody()).compareTo(typedOther.isSetBody());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBody()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.body, typedOther.body);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNanos()).compareTo(typedOther.isSetNanos());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNanos()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.nanos, typedOther.nanos);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetHost()).compareTo(typedOther.isSetHost());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHost()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.host, typedOther.host);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFields()).compareTo(typedOther.isSetFields());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFields()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.fields, typedOther.fields);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ThriftFlumeEvent(");
    boolean first = true;

    sb.append("timestamp:");
    sb.append(this.timestamp);
    first = false;
    if (!first) sb.append(", ");
    sb.append("priority:");
    if (this.priority == null) {
      sb.append("null");
    } else {
      sb.append(this.priority);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("body:");
    if (this.body == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.body, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("nanos:");
    sb.append(this.nanos);
    first = false;
    if (!first) sb.append(", ");
    sb.append("host:");
    if (this.host == null) {
      sb.append("null");
    } else {
      sb.append(this.host);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("fields:");
    if (this.fields == null) {
      sb.append("null");
    } else {
      sb.append(this.fields);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ThriftFlumeEventStandardSchemeFactory implements SchemeFactory {
    public ThriftFlumeEventStandardScheme getScheme() {
      return new ThriftFlumeEventStandardScheme();
    }
  }

  private static class ThriftFlumeEventStandardScheme extends StandardScheme<ThriftFlumeEvent> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ThriftFlumeEvent struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.timestamp = iprot.readI64();
              struct.setTimestampIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PRIORITY
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.priority = Priority.findByValue(iprot.readI32());
              struct.setPriorityIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // BODY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.body = iprot.readBinary();
              struct.setBodyIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // NANOS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.nanos = iprot.readI64();
              struct.setNanosIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // HOST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.host = iprot.readString();
              struct.setHostIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // FIELDS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map0 = iprot.readMapBegin();
                struct.fields = new HashMap<String,ByteBuffer>(2*_map0.size);
                for (int _i1 = 0; _i1 < _map0.size; ++_i1)
                {
                  String _key2; // required
                  ByteBuffer _val3; // required
                  _key2 = iprot.readString();
                  _val3 = iprot.readBinary();
                  struct.fields.put(_key2, _val3);
                }
                iprot.readMapEnd();
              }
              struct.setFieldsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ThriftFlumeEvent struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(struct.timestamp);
      oprot.writeFieldEnd();
      if (struct.priority != null) {
        oprot.writeFieldBegin(PRIORITY_FIELD_DESC);
        oprot.writeI32(struct.priority.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.body != null) {
        oprot.writeFieldBegin(BODY_FIELD_DESC);
        oprot.writeBinary(struct.body);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(NANOS_FIELD_DESC);
      oprot.writeI64(struct.nanos);
      oprot.writeFieldEnd();
      if (struct.host != null) {
        oprot.writeFieldBegin(HOST_FIELD_DESC);
        oprot.writeString(struct.host);
        oprot.writeFieldEnd();
      }
      if (struct.fields != null) {
        oprot.writeFieldBegin(FIELDS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.fields.size()));
          for (Map.Entry<String, ByteBuffer> _iter4 : struct.fields.entrySet())
          {
            oprot.writeString(_iter4.getKey());
            oprot.writeBinary(_iter4.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ThriftFlumeEventTupleSchemeFactory implements SchemeFactory {
    public ThriftFlumeEventTupleScheme getScheme() {
      return new ThriftFlumeEventTupleScheme();
    }
  }

  private static class ThriftFlumeEventTupleScheme extends TupleScheme<ThriftFlumeEvent> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ThriftFlumeEvent struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetTimestamp()) {
        optionals.set(0);
      }
      if (struct.isSetPriority()) {
        optionals.set(1);
      }
      if (struct.isSetBody()) {
        optionals.set(2);
      }
      if (struct.isSetNanos()) {
        optionals.set(3);
      }
      if (struct.isSetHost()) {
        optionals.set(4);
      }
      if (struct.isSetFields()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetTimestamp()) {
        oprot.writeI64(struct.timestamp);
      }
      if (struct.isSetPriority()) {
        oprot.writeI32(struct.priority.getValue());
      }
      if (struct.isSetBody()) {
        oprot.writeBinary(struct.body);
      }
      if (struct.isSetNanos()) {
        oprot.writeI64(struct.nanos);
      }
      if (struct.isSetHost()) {
        oprot.writeString(struct.host);
      }
      if (struct.isSetFields()) {
        {
          oprot.writeI32(struct.fields.size());
          for (Map.Entry<String, ByteBuffer> _iter5 : struct.fields.entrySet())
          {
            oprot.writeString(_iter5.getKey());
            oprot.writeBinary(_iter5.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ThriftFlumeEvent struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
      if (incoming.get(0)) {
        struct.timestamp = iprot.readI64();
        struct.setTimestampIsSet(true);
      }
      if (incoming.get(1)) {
        struct.priority = Priority.findByValue(iprot.readI32());
        struct.setPriorityIsSet(true);
      }
      if (incoming.get(2)) {
        struct.body = iprot.readBinary();
        struct.setBodyIsSet(true);
      }
      if (incoming.get(3)) {
        struct.nanos = iprot.readI64();
        struct.setNanosIsSet(true);
      }
      if (incoming.get(4)) {
        struct.host = iprot.readString();
        struct.setHostIsSet(true);
      }
      if (incoming.get(5)) {
        {
          org.apache.thrift.protocol.TMap _map6 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.fields = new HashMap<String,ByteBuffer>(2*_map6.size);
          for (int _i7 = 0; _i7 < _map6.size; ++_i7)
          {
            String _key8; // required
            ByteBuffer _val9; // required
            _key8 = iprot.readString();
            _val9 = iprot.readBinary();
            struct.fields.put(_key8, _val9);
          }
        }
        struct.setFieldsIsSet(true);
      }
    }
  }

}

