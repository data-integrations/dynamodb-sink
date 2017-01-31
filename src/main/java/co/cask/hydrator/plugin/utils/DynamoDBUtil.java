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
package co.cask.hydrator.plugin.utils;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility methods required for client and item creation.
 */
public final class DynamoDBUtil {

  public static final String CHARACTER_ENCODING = "UTF-8";

  private DynamoDBUtil() { }

  public static int getItemSizeBytes(Map<String, AttributeValue> item) {
    try {
      int itemSize = 0;
      for (Entry<String, AttributeValue> entry : item.entrySet()) {
        itemSize += entry.getKey().getBytes(CHARACTER_ENCODING).length;
        itemSize += getAttributeSizeBytes(entry.getValue());
      }
      return itemSize;
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static int getAttributeSizeBytes(AttributeValue att) throws UnsupportedEncodingException {
    int byteSize = 0;
    if (att.getN() != null) {
      byteSize += att.getN().getBytes(CHARACTER_ENCODING).length;
    } else if (att.getS() != null) {
      byteSize += att.getS().getBytes(CHARACTER_ENCODING).length;
    } else if (att.getB() != null) {
      byteSize += att.getB().array().length;
    } else if (att.getNS() != null) {
      for (String number : att.getNS()) {
        byteSize += number.getBytes(CHARACTER_ENCODING).length;
      }
    } else if (att.getSS() != null) {
      for (String string : att.getSS()) {
        byteSize += string.getBytes(CHARACTER_ENCODING).length;
      }
    } else if (att.getBS() != null) {
      for (ByteBuffer byteBuffer : att.getBS()) {
        byteSize += byteBuffer.array().length;
      }
    }
    return byteSize;
  }

  public static AmazonDynamoDBClient getDynamoDBClient(Configuration conf) {
    BasicAWSCredentials credentials = new BasicAWSCredentials(conf.get(DynamoDBConstants.DYNAMODB_ACCESS_KEY),
                                                              conf.get(DynamoDBConstants.DYNAMODB_SECRET_KEY));
    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
    if (Strings.isNullOrEmpty(conf.get(DynamoDBConstants.REGION_ID))) {
      dynamoDBClient = dynamoDBClient.withRegion(Regions.DEFAULT_REGION);
    } else {
      dynamoDBClient = dynamoDBClient.withRegion(RegionUtils.getRegion(conf.get(DynamoDBConstants.REGION_ID)));
    }
    if (!Strings.isNullOrEmpty(conf.get(DynamoDBConstants.ENDPOINT))) {
      dynamoDBClient.setEndpoint(conf.get(DynamoDBConstants.ENDPOINT));
    }
    return dynamoDBClient;
  }
}
