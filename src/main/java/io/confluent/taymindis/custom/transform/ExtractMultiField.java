/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.taymindis.custom.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class ExtractMultiField<R extends ConnectRecord<R>> implements Transformation<R> {

  //private static final Logger log = LoggerFactory.getLogger(ExtractMultiField.class);

  public static final String OVERVIEW_DOC =
      "Extract the specified field by topic mapping from a Struct when schema present, "
          + "or a Map in the case of schemaless data. "
          + "Any null values are passed through unmodified."
          + "<p/>Use the concrete transformation type designed for the record"
          + "key (<code>" + Key.class.getName() + "</code>) "
          + "or value (<code>" + Value.class.getName() + "</code>).";

  private static final String FIELD_CONFIG = "fields";
  private static final String TOPIC_PREFIX_CONFIG = "topicPrefix";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FIELD_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
          ConfigDef.Importance.MEDIUM, "Fields name by topic to extract.")
      .define(TOPIC_PREFIX_CONFIG, ConfigDef.Type.STRING, "",
          ConfigDef.Importance.MEDIUM, "Topic Prefix ");

  private static final String PURPOSE = "fields extraction based on topic";

  private Map<String, String> fieldNameTopicMapping;
  private String topicPrefix;

  @Override
  public void configure(Map<String, ?> props) {
    fieldNameTopicMapping = new HashMap<>();
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    List<String> fieldNames = config.getList(FIELD_CONFIG);
    topicPrefix = config.getString(TOPIC_PREFIX_CONFIG);
    for (String fieldName : fieldNames) {
      String[] topicField = fieldName.split(":");
      if (topicField.length != 2) {
        throw new ConfigException("topic field names mapping",
            topicField, "Invalid topic field names mapping: " + Arrays.toString(topicField));
      }
      // TODO Rebuild and test again and sink with key integer converter if possible for primary key
      fieldNameTopicMapping.put(topicPrefix.concat(topicField[0]), topicField[1]);
    }
  }

  @Override
  public R apply(R record) {
    final Schema schema = operatingSchema(record);
    //  log.info("Topic Target {}", record.topic());
    final String fieldName = fieldNameTopicMapping.get(record.topic());
    //  log.info("FieldName Target {}", fieldName);

    if (fieldName == null) {
      throw new IllegalArgumentException("Unable to get field from topic: " + record.topic());
    }

    if (schema == null) {
      final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
      return newRecord(record, null, value == null ? null : value.get(fieldName));
    } else {
      final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
      Field field = schema.field(fieldName);

      if (field == null) {
        throw new IllegalArgumentException("Unknown field: " + fieldName);
      }

      return newRecord(record, field.schema(), value == null ? null : value.get(fieldName));
    }
  }

  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends ExtractMultiField<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(),
          updatedSchema, updatedValue, record.valueSchema(),
          record.value(), record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends ExtractMultiField<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(),
          record.keySchema(), record.key(), updatedSchema,
          updatedValue, record.timestamp());
    }
  }

}
