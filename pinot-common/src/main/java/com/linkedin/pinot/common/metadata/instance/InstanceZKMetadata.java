/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata.instance;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.metadata.ZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.StringUtil;


public class InstanceZKMetadata implements ZKMetadata {

  private static String KAFKA_HIGH_LEVEL_CONSUMER_GROUP_MAP = "KAFKA_HLC_GROUP_MAP";
  private static String KAFKA_HIGH_LEVEL_CONSUMER_PARTITION_MAP = "KAFKA_HLC_PARTITION_MAP";
  private String _id = null;
  private String _instanceName = null;
  private int _instancePort;
  private String _instanceType = null;

  // Only care about realtime resources for now
  private Map<String, String> _groupIdMap = new HashMap<String, String>();
  private Map<String, String> _partitionMap = new HashMap<String, String>();

  public InstanceZKMetadata() {
  }

  public InstanceZKMetadata(ZNRecord record) {
    _id = record.getId();
    setInstanceConfigFromId(_id);
    _groupIdMap.putAll(record.getMapField(KAFKA_HIGH_LEVEL_CONSUMER_GROUP_MAP));
    _partitionMap.putAll(record.getMapField(KAFKA_HIGH_LEVEL_CONSUMER_PARTITION_MAP));
  }

  private void setInstanceConfigFromId(String id) {
    String[] instanceConfigs = id.split("_");
    assert instanceConfigs.length == 3;
    setInstanceType(instanceConfigs[0]);
    setInstanceName(instanceConfigs[1]);
    setInstancePort(Integer.parseInt(instanceConfigs[2]));
  }

  public int getInstancePort() {
    return _instancePort;
  }

  public void setInstancePort(int instancePort) {
    _instancePort = instancePort;
  }

  public String getInstanceType() {
    return _instanceType;
  }

  public void setInstanceType(String instanceType) {
    _instanceType = instanceType;
  }

  public String getId() {
    if (_id == null) {
      _id = buildIdFromInstanceConfig();
    }
    return _id;
  }

  public void setId(String id) {
    _id = id;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  public String getGroupId(String resourceName) {
    return _groupIdMap.get(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName));
  }

  public void setGroupId(String resourceName, String groupId) {
    _groupIdMap.put(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName), groupId);
  }

  public String getPartition(String resourceName) {
    return _partitionMap.get(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName));
  }

  public void setPartition(String resourceName, String partition) {
    _partitionMap.put(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName), partition);
  }

  public void removeResource(String resourceName) {
    _groupIdMap.remove(resourceName);
    _partitionMap.remove(resourceName);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(getId());
    znRecord.setMapField(KAFKA_HIGH_LEVEL_CONSUMER_GROUP_MAP, _groupIdMap);
    znRecord.setMapField(KAFKA_HIGH_LEVEL_CONSUMER_PARTITION_MAP, _partitionMap);
    return znRecord;
  }

  private String buildIdFromInstanceConfig() {
    return StringUtil.join("_", _instanceType, _instanceName, _instancePort + "");
  }

  @Override
  public boolean equals(Object instanceMetadata) {
    if (!(instanceMetadata instanceof InstanceZKMetadata)) {
      return false;
    }

    InstanceZKMetadata metadata = (InstanceZKMetadata) instanceMetadata;
    if (!_instanceName.equals(metadata.getInstanceName()) ||
        !_instanceType.equals(metadata.getInstanceType()) ||
        _instancePort != metadata.getInstancePort()) {
      return false;
    }
    for (String resourceName : _groupIdMap.keySet()) {
      if (!getGroupId(resourceName).equals(metadata.getGroupId(resourceName))) {
        return false;
      }
      if (!getPartition(resourceName).equals(metadata.getPartition(resourceName))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = _id != null ? _id.hashCode() : 0;
    result = 31 * result + (_instanceName != null ? _instanceName.hashCode() : 0);
    result = 31 * result + _instancePort;
    result = 31 * result + (_instanceType != null ? _instanceType.hashCode() : 0);
    result = 31 * result + (_groupIdMap != null ? _groupIdMap.hashCode() : 0);
    result = 31 * result + (_partitionMap != null ? _partitionMap.hashCode() : 0);
    return result;
  }
}
