/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Config class for Batch DynamoDB Sink
 */
public class DynamoDBBatchSinkConfig extends ReferencePluginConfig {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_ACCESS_KEY = "secretAccessKey";
  public static final String REGION_ID = "regionId";
  public static final String ENDPOINT_URL = "endpointUrl";
  public static final String TABLE_NAME = "tableName";
  public static final String PRIMARY_KEY_FIELDS = "primaryKeyFields";
  public static final String PRIMARY_KEY_TYPES = "primaryKeyTypes";
  public static final String READ_CAPACITY_UNITS = "readCapacityUnits";
  public static final String WRITE_CAPACITY_UNITS = "writeCapacityUnits";

  @Name(ACCESS_KEY)
  @Macro
  @Description("The access Id provided by AWS required to access the DynamoDb tables. (Macro Enabled)")
  private String accessKey;

  @Name(SECRET_ACCESS_KEY)
  @Macro
  @Description("AWS access key secret having access to DynamoDb tables. (Macro Enabled)")
  private String secretAccessKey;

  @Name(REGION_ID)
  @Nullable
  @Description("The region for AWS Dynamo DB to connect to. Default is us-west-2 i.e. US West (Oregon).")
  private String regionId;

  @Name(ENDPOINT_URL)
  @Description("The hostname and port for AWS DynamoDB instance to connect to, separated by a colon. If not " +
    "provided, it will be constructed using regionId. Eg. dynamodb.us-east-1.amazonaws.com.")
  @Nullable
  private String endpointUrl;

  @Name(TABLE_NAME)
  @Description("The table to write the data to. If the specified table does not exists, it will be created using " +
    "the primary key attributes and key schema and the read and write capacity units.")
  private String tableName;

  @Name(PRIMARY_KEY_FIELDS)
  @Description("A comma-separated list of key-value pairs representing the primary key and its attribute type. " +
    "The value can be of the following type: 'N'(number), 'S'(string) and 'B'(binary - the byte[] value received " +
    "from the previous stage will be converted to binary when storing the data in DynamoDB). Eg. \"Id:N,Region:S\"")
  private String primaryKeyFields;

  @Name(PRIMARY_KEY_TYPES)
  @Description("A comma-separated list of key-value pairs representing the primary key and its attribute type.\n" +
    "The key type can have the following values: 'HASH' or 'RANGE'. Eg. \"Id:HASH,Region:HASH\".")
  private String primaryKeyTypes;

  @Name(READ_CAPACITY_UNITS)
  @Nullable
  @Description("The maximum number of strongly consistent reads consumed per second before DynamoDB returns a " +
    "ThrottlingException. This will be used when creating a new table if the table name specified by the user does " +
    "not exists.")
  private String readCapacityUnits;

  @Name(WRITE_CAPACITY_UNITS)
  @Nullable
  @Description("The maximum number of writes consumed per second before DynamoDB returns a ThrottlingException. " +
    "This will be used when creating a new table if the table name specified by the user does not exists.")
  private String writeCapacityUnits;

  public DynamoDBBatchSinkConfig(String referenceName, String accessKey, String secretAccessKey,
                                 @Nullable String endpointUrl, @Nullable String regionId, String tableName,
                                 String primaryKeyFields, String primaryKeyTypes, @Nullable String readCapacityUnits,
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

  private DynamoDBBatchSinkConfig(Builder builder) {
    super(builder.referenceName);
    accessKey = builder.accessKey;
    secretAccessKey = builder.secretAccessKey;
    regionId = builder.regionId;
    endpointUrl = builder.endpointUrl;
    tableName = builder.tableName;
    primaryKeyFields = builder.primaryKeyFields;
    primaryKeyTypes = builder.primaryKeyTypes;
    readCapacityUnits = builder.readCapacityUnits;
    writeCapacityUnits = builder.writeCapacityUnits;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(DynamoDBBatchSinkConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setAccessKey(copy.getAccessKey())
      .setSecretAccessKey(copy.getSecretAccessKey())
      .setRegionId(copy.getRegionId())
      .setEndpointUrl(copy.getEndpointUrl())
      .setTableName(copy.getTableName())
      .setPrimaryKeyFields(copy.getPrimaryKeyFields())
      .setPrimaryKeyTypes(copy.getPrimaryKeyTypes())
      .setReadCapacityUnits(copy.getReadCapacityUnits())
      .setWriteCapacityUnits(copy.getWriteCapacityUnits());
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  @Nullable
  public String getRegionId() {
    return regionId;
  }

  @Nullable
  public String getEndpointUrl() {
    return endpointUrl;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPrimaryKeyFields() {
    return primaryKeyFields;
  }

  public String getPrimaryKeyTypes() {
    return primaryKeyTypes;
  }

  @Nullable
  public String getReadCapacityUnits() {
    return readCapacityUnits;
  }

  @Nullable
  public String getWriteCapacityUnits() {
    return writeCapacityUnits;
  }

  /**
   * Validates whether the table name follows the DynamoDB naming rules and conventions or not.
   */
  public void validateTableName(FailureCollector failureCollector) {
    int tableNameLength = tableName.length();
    if (tableNameLength < 3 || tableNameLength > 255) {
      failureCollector.addFailure(
        String.format("Table name '%s' does not follow the DynamoDB naming rules.", tableName),
        "Table name must be between 3 and 255 characters long.")
        .withConfigProperty(TABLE_NAME);
    }

    String pattern = "^[a-zA-Z0-9_.-]+$";
    Pattern patternObj = Pattern.compile(pattern);

    if (!Strings.isNullOrEmpty(tableName)) {
      Matcher matcher = patternObj.matcher(tableName);
      if (!matcher.find()) {
        failureCollector.addFailure(
          String.format("Table name '%s' does not follow the DynamoDB naming rules.", tableName),
          "Table names can contain only the following characters: 'a-z, A-Z, 0-9, underscore(_), " +
            "dot(.) and dash(-)'.")
          .withConfigProperty(TABLE_NAME);
      }

    }
  }

  /**
   * Validates the partition and sort key provided by the user.
   */
  public void validatePrimaryKey(FailureCollector failureCollector, Schema inputSchema) {
    Set<String> primaryKeyAttributes = getPrimaryKeyFieldsSet();
    Set<String> primaryKeySchema = getPrimaryKeyTypesSet();

    if (primaryKeyAttributes.size() > 2 || primaryKeySchema.size() > 2) {
      failureCollector.addFailure("Invalid primary key.",
                                  "Primary key can have only 2 values HASH and RANGE.")
        .withConfigProperty(PRIMARY_KEY_FIELDS);
    } else if (primaryKeyAttributes.size() < 1 || primaryKeySchema.size() < 1) {
      failureCollector.addFailure("Invalid primary key.",
                                  "Primary key should have at least one value.")
        .withConfigProperty(PRIMARY_KEY_FIELDS);
    } else if (!primaryKeyAttributes.equals(primaryKeySchema)) {
      failureCollector.addFailure(
        String.format("Key type for %s is not specified.",
                      Sets.difference(primaryKeyAttributes, primaryKeySchema)),
        "Please specify attribute value and key types for the same primary keys.")
        .withConfigProperty(PRIMARY_KEY_TYPES);
    } else {
      for (String field : primaryKeyAttributes) {
        if (inputSchema.getField(field).getSchema().isNullable()
          || inputSchema.getField(field).getSchema().getType() == Schema.Type.NULL) {
          failureCollector.addFailure(
            String.format("Attribute %s was found to be of nullable type schema.", field),
            "Attribute values cannot be null.")
            .withInputSchemaField(field, null);
        }
      }
    }
  }

  public Set<String> getPrimaryKeyTypesSet() {
    return Splitter.on(',').trimResults().withKeyValueSeparator(":")
      .split(primaryKeyTypes).keySet();
  }

  public Set<String> getPrimaryKeyFieldsSet() {
    return Splitter.on(',').trimResults().withKeyValueSeparator(":")
      .split(primaryKeyFields).keySet();
  }

  /**
   * Builder for creating a {@link DynamoDBBatchSinkConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String accessKey;
    private String secretAccessKey;
    private String regionId;
    private String endpointUrl;
    private String tableName;
    private String primaryKeyFields;
    private String primaryKeyTypes;
    private String readCapacityUnits;
    private String writeCapacityUnits;

    private Builder() {
    }

    public Builder setReferenceName(String val) {
      referenceName = val;
      return this;
    }

    public Builder setAccessKey(String val) {
      accessKey = val;
      return this;
    }

    public Builder setSecretAccessKey(String val) {
      secretAccessKey = val;
      return this;
    }

    public Builder setRegionId(String val) {
      regionId = val;
      return this;
    }

    public Builder setEndpointUrl(String val) {
      endpointUrl = val;
      return this;
    }

    public Builder setTableName(String val) {
      tableName = val;
      return this;
    }

    public Builder setPrimaryKeyFields(String val) {
      primaryKeyFields = val;
      return this;
    }

    public Builder setPrimaryKeyTypes(String val) {
      primaryKeyTypes = val;
      return this;
    }

    public Builder setReadCapacityUnits(String val) {
      readCapacityUnits = val;
      return this;
    }

    public Builder setWriteCapacityUnits(String val) {
      writeCapacityUnits = val;
      return this;
    }

    public DynamoDBBatchSinkConfig build() {
      return new DynamoDBBatchSinkConfig(this);
    }
  }
}
