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

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import io.cdap.plugin.utils.DynamoDBConstants;
import io.cdap.plugin.utils.DynamoDBUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Record Writer for DynamoDB Sink.
 */
public class DynamoDBRecordWriter extends RecordWriter<NullWritable, Item> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBRecordWriter.class);
  int batchSizeBytes = 0;
  private DynamoDB dynamoDB;
  private String tableName;
  private List<Item> itemList = new ArrayList<>();

  public DynamoDBRecordWriter(TaskAttemptContext context) throws IOException {
    dynamoDB = new DynamoDB(DynamoDBUtil.getDynamoDBClient(context.getConfiguration()));
    tableName = context.getConfiguration().get(DynamoDBConstants.OUTPUT_TABLE_NAME);
  }

  @Override
  public void write(NullWritable key, Item item) throws IOException, InterruptedException {
    if (item == null) {
      throw new RuntimeException("Null record encountered. At least the key columns must be specified.");
    }
    Map<String, AttributeValue> attributes = InternalUtils.toAttributeValues(item);
    int itemSizeBytes = DynamoDBUtil.getItemSizeBytes(attributes);
    if (itemSizeBytes > DynamoDBConstants.MAX_ALLOWABLE_BYTE_SIZE) {
      throw new RuntimeException(String.format("Cannot pass items with size greater than %s bytes",
                                               DynamoDBConstants.MAX_ALLOWABLE_BYTE_SIZE));
    }
    batchSizeBytes += itemSizeBytes;

    itemList.add(item);

    if (itemList.size() == DynamoDBConstants.MAX_WRITE_BATCH_SIZE
      || batchSizeBytes >= DynamoDBConstants.MAX_BATCH_SIZE_BYTES) {
      putBatch(itemList);
      itemList.clear();
      batchSizeBytes = 0;
    }
  }

  private void putBatch(List<Item> items) {
    TableWriteItems tableWriteItems = new TableWriteItems(tableName).withItemsToPut(items);
    for (int i = 0; i < DynamoDBConstants.MAX_RETIRES; i++) {
      try {
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);
        do {
          // Check for unprocessed keys which could happen if you exceed provisioned throughput
          Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
          if (outcome.getUnprocessedItems().size() == 0) {
            LOG.info("No unprocessed items found");
          } else {
            LOG.info("Retrieving the unprocessed items");
            outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
          }
        } while (outcome.getUnprocessedItems().size() > 0);
        return;
      } catch (ProvisionedThroughputExceededException ex) {
        LOG.warn("Throughput exceeded. Sleeping for 10 ms and retrying.");
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Retry wait interrupted", e);
        }
      }
    }
    LOG.error("Maximum retries exhausted");
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    try {
      if (!itemList.isEmpty()) {
        putBatch(itemList);
      }
    } finally {
      if (dynamoDB != null) {
        dynamoDB.shutdown();
      }
      itemList.clear();
    }
  }
}
