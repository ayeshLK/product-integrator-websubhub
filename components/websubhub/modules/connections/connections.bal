// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import websubhub.config;

import ballerinax/kafka;
import websubhub.common;

// Producer which persist the current in-memory state of the Hub 
kafka:ProducerConfiguration statePersistConfig = {
    clientId: "state-persist",
    acks: "1",
    retryCount: 3,
    secureSocket: config:kafka.connection.secureSocket,
    securityProtocol: config:kafka.connection.securityProtocol
};
public final kafka:Producer statePersistProducer = check new (config:kafka.connection.bootstrapServers, statePersistConfig);

// Consumer which reads the persisted subscriber details
kafka:ConsumerConfiguration websubEventsConsumerConfig = {
    groupId: config:state.events.consumerGroup,
    offsetReset: "earliest",
    topics: [config:state.events.topic],
    secureSocket: config:kafka.connection.secureSocket,
    securityProtocol: config:kafka.connection.securityProtocol
};
public final kafka:Consumer websubEventsConsumer = check new (config:kafka.connection.bootstrapServers, websubEventsConsumerConfig);

# Creates a `kafka:Consumer` for a subscriber.
#
# + topicName - The kafka-topic to which the consumer should subscribe  
# + groupName - The consumer group name  
# + partitions - The kafka topic-partitions
# + return - `kafka:Consumer` if succcessful or else `error`
public isolated function createMessageConsumer(string topicName, string groupName, int[]? partitions = ()) returns kafka:Consumer|error {
    kafka:ConsumerConfiguration consumerConfiguration = {
        groupId: groupName,
        autoCommit: false,
        secureSocket: config:kafka.connection.secureSocket,
        securityProtocol: config:kafka.connection.securityProtocol,
        maxPollRecords: config:kafka.consumer.maxPollRecords
    };
    if partitions is () {
        // Kafka consumer topic subscription should only be used when manual partition assignment is not used
        consumerConfiguration.topics = [topicName];
        return new (config:kafka.connection.bootstrapServers, consumerConfiguration);
    }

    kafka:Consumer|kafka:Error consumerEp = check new (config:kafka.connection.bootstrapServers, consumerConfiguration);
    if consumerEp is kafka:Error {
        common:logError("Error occurred while creating the consumer", consumerEp);
        return consumerEp;
    }

    kafka:TopicPartition[] kafkaTopicPartitions = partitions.'map(p => {topic: topicName, partition: p});
    kafka:Error? paritionAssignmentErr = consumerEp->assign(kafkaTopicPartitions);
    if paritionAssignmentErr is kafka:Error {
        common:logError("Error occurred while assigning partitions to the consumer", paritionAssignmentErr);
        return paritionAssignmentErr;
    }

    kafka:TopicPartition[] parititionsWithoutCmtdOffsets = [];
    foreach kafka:TopicPartition partition in kafkaTopicPartitions {
        kafka:PartitionOffset|kafka:Error? offset = consumerEp->getCommittedOffset(partition);
        if offset is kafka:Error {
            common:logError("Error occurred while retrieving the commited offsets for the topic-partition", offset);
            return offset;
        }

        if offset is () {
            parititionsWithoutCmtdOffsets.push(partition);
        }

        if offset is kafka:PartitionOffset {
            kafka:Error? kafkaSeekErr = consumerEp->seek(offset);
            if kafkaSeekErr is kafka:Error {
                common:logError("Error occurred while assigning seeking partitions for the consumer", kafkaSeekErr);
                return kafkaSeekErr;
            }
        }
    }

    if parititionsWithoutCmtdOffsets.length() > 0 {
        kafka:Error? kafkaSeekErr = consumerEp->seekToBeginning(parititionsWithoutCmtdOffsets);
        if kafkaSeekErr is kafka:Error {
            common:logError("Error occurred while assigning seeking partitions (for paritions without committed offsets) for the consumer", kafkaSeekErr);
            return kafkaSeekErr;
        }
    }
    return consumerEp;
}
