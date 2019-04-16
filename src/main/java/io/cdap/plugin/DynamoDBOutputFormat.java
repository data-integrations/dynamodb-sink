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
package io.cdap.plugin;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.google.common.base.Splitter;
import io.cdap.plugin.utils.DynamoDBConstants;
import io.cdap.plugin.utils.DynamoDBUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * OutputFormat for DynamoDB Sink.
 */
public class DynamoDBOutputFormat extends OutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBOutputFormat.class);
  private static AmazonDynamoDBClient dynamoDBClient;

  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new DynamoDBRecordWriter(taskAttemptContext);
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    Configuration conf = context.getConfiguration();
    try {
      if (dynamoDBClient == null) {
        dynamoDBClient = DynamoDBUtil.getDynamoDBClient(conf);
        if (!checkIfTableExists(conf)) {
          ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
          Map<String, String> primaryKeyAttribues = Splitter.on(',').trimResults().withKeyValueSeparator(":")
            .split(conf.get(DynamoDBConstants.PRIMARY_KEY_FIELDS));

          for (Map.Entry<String, String> entry : primaryKeyAttribues.entrySet()) {
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(entry.getKey())
                                       .withAttributeType(entry.getValue().toUpperCase()));
          }
          ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
          Map<String, String> primaryKeyTypes = Splitter.on(',').trimResults().withKeyValueSeparator(":")
            .split(conf.get(DynamoDBConstants.PRIMARY_KEY_TYPES));

          for (Map.Entry<String, String> entry : primaryKeyTypes.entrySet()) {
            keySchema.add(new KeySchemaElement().withAttributeName(entry.getKey())
                            .withKeyType(KeyType.valueOf(entry.getValue().toUpperCase())));
          }

          CreateTableRequest request = new CreateTableRequest()
            .withTableName(conf.get(DynamoDBConstants.OUTPUT_TABLE_NAME))
            .withKeySchema(keySchema)
            .withAttributeDefinitions(attributeDefinitions)
            .withProvisionedThroughput(
              new ProvisionedThroughput()
                .withReadCapacityUnits(conf.getLong(DynamoDBConstants.READ_CAPACITY_UNITS,
                                                    Long.parseLong(DynamoDBConstants.DEFAULT_READ_CAPACITY_UNITS)))
                .withWriteCapacityUnits(conf.getLong(DynamoDBConstants.WRITE_CAPACITY_UNITS,
                                                     Long.parseLong(DynamoDBConstants.DEFAULT_WRITE_CAPACITY_UNITS))));
          Table table = new DynamoDB(dynamoDBClient).createTable(request);
          LOG.info("Creating table {}, with attributes {}.", conf.get(DynamoDBConstants.OUTPUT_TABLE_NAME),
                   primaryKeyAttribues.keySet());
          try {
            table.waitForActive();
          } catch (InterruptedException e) {
            throw new IllegalArgumentException(String.format("Error creating table %s.",
                                                             conf.get(DynamoDBConstants.OUTPUT_TABLE_NAME)));
          }
        }
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Cannot connect to dynamodb instance. %s.", e.getMessage()), e);
    }
  }

  private boolean checkIfTableExists(Configuration conf) {
    try {
      TableDescription table = dynamoDBClient.describeTable(
        new DescribeTableRequest(conf.get(DynamoDBConstants.OUTPUT_TABLE_NAME))).getTable();
      return TableStatus.ACTIVE.toString().equals(table.getTableStatus());
    } catch (ResourceNotFoundException rnfe) {
      // This means the table doesn't exist in the account yet
      return false;
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    return new NoOpOutputCommitter();
  }
}
