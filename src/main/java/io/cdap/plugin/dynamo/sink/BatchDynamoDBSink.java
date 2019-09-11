/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.dynamo.sink;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.dynamo.sink.utils.DynamoDBConstants;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A {@link BatchSink} that writes data to DynamoDB.
 * This {@link BatchDynamoDBSink} takes a {@link StructuredRecord} in, converts it to Item, and writes it to the
 * DynamoDB table.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(BatchDynamoDBSink.PLUGIN_NAME)
@Description("CDAP Amazon DynamoDB Batch Sink takes the structured record from the input source and writes the data " +
  "into Amazon DynamoDB.")
public class BatchDynamoDBSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Item> {
  public static final String PLUGIN_NAME = "DynamoDB";
  private final DynamoDBBatchSinkConfig config;

  public BatchDynamoDBSink(DynamoDBBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validateTableName(failureCollector);
    config.validatePrimaryKey(failureCollector, pipelineConfigurer.getStageConfigurer().getInputSchema());
    failureCollector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    context.addOutput(Output.of(config.referenceName, new DynamoDBOutputFormatProvider(config)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Item>> emitter) throws Exception {
    Set<String> primaryKeyAttributes = config.getPrimaryKeyFieldsSet();
    PrimaryKey primaryKey = new PrimaryKey();
    Item item = new Item();
    for (Schema.Field field : input.getSchema().getFields()) {
      String fieldName = field.getName();
      if (primaryKeyAttributes.contains(field.getName())) {
        if (input.get(fieldName) == null || Strings.isNullOrEmpty(String.valueOf(input.get(fieldName)))) {
          throw new IllegalArgumentException(String.format("Attribute values cannot be null or empty. Null value " +
                                                             "found for attribute %s.", fieldName));
        }
        primaryKey.addComponent(fieldName, input.get(fieldName));
      } else {
        item.with(fieldName, input.get(fieldName));
      }
    }
    item.withPrimaryKey(primaryKey);
    emitter.emit(new KeyValue<>(NullWritable.get(), item));
  }

  private static class DynamoDBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DynamoDBOutputFormatProvider(DynamoDBBatchSinkConfig config) {
      this.conf = new HashMap<>();
      conf.put(DynamoDBConstants.OUTPUT_TABLE_NAME, config.getTableName());
      conf.put(DynamoDBConstants.REGION_ID, config.getRegionId() == null ? "" : config.getRegionId());
      conf.put(DynamoDBConstants.ENDPOINT, config.getEndpointUrl() == null ? "" : config.getEndpointUrl());
      conf.put(DynamoDBConstants.DYNAMODB_ACCESS_KEY, config.getAccessKey());
      conf.put(DynamoDBConstants.DYNAMODB_SECRET_KEY, config.getSecretAccessKey());
      conf.put(DynamoDBConstants.PRIMARY_KEY_FIELDS, config.getPrimaryKeyFields());
      conf.put(DynamoDBConstants.PRIMARY_KEY_TYPES, config.getPrimaryKeyTypes());
      conf.put(DynamoDBConstants.READ_CAPACITY_UNITS, config.getReadCapacityUnits() == null ? "" :
        config.getReadCapacityUnits());
      conf.put(DynamoDBConstants.WRITE_CAPACITY_UNITS, config.getWriteCapacityUnits() == null ? "" :
        config.getWriteCapacityUnits());
    }

    @Override
    public String getOutputFormatClassName() {
      return DynamoDBOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
