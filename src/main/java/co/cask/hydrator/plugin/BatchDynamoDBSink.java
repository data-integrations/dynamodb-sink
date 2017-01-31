/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.utils.DynamoDBConstants;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

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
  private final DynamoDBConfig config;

  public BatchDynamoDBSink(DynamoDBConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validateTableName();
    config.validatePrimaryKey(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    context.addOutput(Output.of(config.referenceName, new DynamoDBOutputFormatProvider(config)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Item>> emitter) throws Exception {
    Set<String> primaryKeyAttributes = Splitter.on(',').trimResults().withKeyValueSeparator(":")
      .split(config.primaryKeyFields).keySet();
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

  /**
   * Config class for Batch DynamoDB Sink
   */
  public static class DynamoDBConfig extends ReferencePluginConfig {
    @Macro
    @Description("The access Id provided by AWS required to access the DynamoDb tables. (Macro Enabled)")
    private String accessKey;

    @Macro
    @Description("AWS access key secret having access to DynamoDb tables. (Macro Enabled)")
    private String secretAccessKey;

    @Nullable
    @Description("The region for AWS Dynamo DB to connect to. Default is us-west-2 i.e. US West (Oregon).")
    private String regionId;

    @Description("The hostname and port for AWS DynamoDB instance to connect to, separated by a colon. If not " +
      "provided, it will be constructed using regionId. Eg. dynamodb.us-east-1.amazonaws.com.")
    @Nullable
    private String endpointUrl;

    @Description("The table to write the data to. If the specified table does not exists, it will be created using " +
      "the primary key attributes and key schema and the read and write capacity units.")
    private String tableName;

    @Description("A comma-separated list of key-value pairs representing the primary key and its attribute type. " +
      "The value can be of the following type: 'N'(number), 'S'(string) and 'B'(binary - the byte[] value received " +
      "from the previous stage will be converted to binary when storing the data in DynamoDB). Eg. \"Id:N,Region:S\"")
    private String primaryKeyFields;

    @Description("A comma-separated list of key-value pairs representing the primary key and its attribute type.\n" +
      "The key type can have the following values: 'HASH' or 'RANGE'. Eg. \"Id:HASH,Region:HASH\".")
    private String primaryKeyTypes;

    @Nullable
    @Description("The maximum number of strongly consistent reads consumed per second before DynamoDB returns a " +
      "ThrottlingException. This will be used when creating a new table if the table name specified by the user does " +
      "not exists.")
    private String readCapacityUnits;

    @Nullable
    @Description("The maximum number of writes consumed per second before DynamoDB returns a ThrottlingException. " +
      "This will be used when creating a new table if the table name specified by the user does not exists.")
    private String writeCapacityUnits;

    public DynamoDBConfig(String referenceName, String accessKey, String secretAccessKey, @Nullable String endpointUrl,
                          @Nullable String regionId, String tableName, String primaryKeyFields,
                          String primaryKeyTypes, @Nullable String readCapacityUnits,
                          @Nullable String writeCapacityUnits) {
      super(referenceName);
      this.endpointUrl = endpointUrl;
      this.regionId = regionId;
      this.accessKey = accessKey;
      this.secretAccessKey = secretAccessKey;
      this.tableName = tableName;
      this.primaryKeyFields = primaryKeyFields;
      this.primaryKeyTypes = primaryKeyTypes;
      this.readCapacityUnits = readCapacityUnits;
      this.writeCapacityUnits = writeCapacityUnits;
    }

    /**
     * Validates whether the table name follows the DynamoDB naming rules and conventions or not.
     */
    private void validateTableName() {
      int tableNameLength = tableName.length();
      if (tableNameLength < 3 || tableNameLength > 255) {
        throw new IllegalArgumentException(
          String.format("Table name '%s' does not follow the DynamoDB naming rules. Table name must be between 3 and " +
                          "255 characters long.", tableName));

      }

      String pattern = "^[a-zA-Z0-9_.-]+$";
      Pattern patternObj = Pattern.compile(pattern);

      if (!Strings.isNullOrEmpty(tableName)) {
        Matcher matcher = patternObj.matcher(tableName);
        if (!matcher.find()) {
          throw new IllegalArgumentException(
            String.format("Table name '%s' does not follow the DynamoDB naming rules. Table names can contain only " +
                            "the following characters: 'a-z, A-Z, 0-9, underscore(_), dot(.) and dash(-)'.",
                          tableName));
        }
      }
    }

    /**
     * Validates the partition and sort key provided by the user.
     */
    private void validatePrimaryKey(Schema inputSchema) {
      Set<String> primaryKeyAttributes = Splitter.on(',').trimResults().withKeyValueSeparator(":")
        .split(primaryKeyFields).keySet();
      Set<String> primaryKeySchema = Splitter.on(',').trimResults().withKeyValueSeparator(":")
        .split(primaryKeyTypes).keySet();

      if (primaryKeyAttributes.size() > 2 || primaryKeySchema.size() > 2) {
        throw new IllegalArgumentException("Primary key can have only 2 values HASH and RANGE.");
      }
      if (primaryKeyAttributes.size() < 1 || primaryKeySchema.size() < 1) {
        throw new IllegalArgumentException("Primary key should have at least one value.");
      }
      if (!primaryKeyAttributes.equals(primaryKeySchema)) {
        throw new IllegalArgumentException(
          String.format("Please specify attribute value and key types for the same primary keys. Key type for %s " +
                          "is not specified.",
                        com.google.common.collect.Sets.difference(primaryKeyAttributes, primaryKeySchema)));
      }

      for (String field : primaryKeyAttributes) {
        if (inputSchema.getField(field).getSchema().isNullable() || inputSchema.getField(field).getSchema().getType() ==
          Schema.Type.NULL) {
          throw new IllegalArgumentException(String.format("Attribute values cannot be null. But attribute %s was " +
                                                             "found to be of nullable type schema.", field));
        }
      }
    }
  }

  private static class DynamoDBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DynamoDBOutputFormatProvider(DynamoDBConfig config) {
      this.conf = new HashMap<>();
      conf.put(DynamoDBConstants.OUTPUT_TABLE_NAME, config.tableName);
      conf.put(DynamoDBConstants.REGION_ID, config.regionId == null ? "" : config.regionId);
      conf.put(DynamoDBConstants.ENDPOINT, config.endpointUrl == null ? "" : config.endpointUrl);
      conf.put(DynamoDBConstants.DYNAMODB_ACCESS_KEY, config.accessKey);
      conf.put(DynamoDBConstants.DYNAMODB_SECRET_KEY, config.secretAccessKey);
      conf.put(DynamoDBConstants.PRIMARY_KEY_FIELDS, config.primaryKeyFields);
      conf.put(DynamoDBConstants.PRIMARY_KEY_TYPES, config.primaryKeyTypes);
      conf.put(DynamoDBConstants.READ_CAPACITY_UNITS, config.readCapacityUnits == null ? "" : config.readCapacityUnits);
      conf.put(DynamoDBConstants.WRITE_CAPACITY_UNITS, config.writeCapacityUnits == null ? "" :
        config.writeCapacityUnits);
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
