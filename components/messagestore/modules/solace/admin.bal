// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
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

import messagestore.api;

import ballerina/http;
import ballerina/log;

import xlibb/solace.semp;

type SolaceQueueNotFound distinct error;

type SolaceEntityNotFound distinct error;

type SolaceQueueExists distinct error;

type SolaceEntityExists distinct error;

public isolated client class Administrator {
    *api:Administrator;

    private final semp:Client administrator;
    private final string messageVpn;

    public isolated function init(Config config) returns error? {
        self.administrator = check new (
            serviceUrl = config.admin.url,
            config = {
                timeout: config.admin.timeout,
                secureSocket: extractSolaceAdminSecureSocketConfig(config.admin.secureSocket),
                auth: config.admin.auth,
                retryConfig: config.admin.retryConfig
            }
        );
        self.messageVpn = config.messageVpn;
    }

    isolated remote function createTopic(string topic, record {} meta = {}) returns api:TopicExists|error? {
        return;
    }

    isolated remote function deleteTopic(string topic, record {} meta = {}) returns api:TopicNotFound|error? {
        return;
    }

    isolated remote function createSubscription(string topic, string queueName, record {} meta = {}) returns api:SubscriptionExists|error? {
        log:printWarn("Creating topic subscription for ", topic = topic, queue = queueName, meta = meta);
        semp:MsgVpnQueue|error queue = self.retrieveQueue(queueName);
        if queue is SolaceQueueNotFound {
            string dlqName = string `dlq-${queueName}`;
            semp:MsgVpnQueue|error dlq = self.retrieveQueue(dlqName);
            if dlq is SolaceQueueNotFound {
                _ = check self.createQueue(dlqName);
            } else if dlq is error {
                return dlq;
            }
            _ = check self.createQueue(queueName, dlqName);
        } else if queue is error {
            return queue;
        }
        _ = check self.addTopicSubscription(queueName, topic);
    }

    isolated remote function deleteSubscription(string topic, string queueName, record {} meta = {}) returns api:SubscriptionNotFound|error? {
        semp:MsgVpnQueueSubscription[]? subscriptions = check self.retrieveTopicSubscriptions(queueName);
        if subscriptions is () {
            return;
        }

        semp:MsgVpnQueueSubscription[] filteredSubscriptions = subscriptions.filter(a => a.subscriptionTopic === topic);
        if filteredSubscriptions.length() === 0 {
            // If the topic-subscription is not-available return nil so that unsubscription not fail
            return;
        }

        _ = check self.removeTopicSubscription(queueName, topic);

        if subscriptions.length() === 1 {
            string dlqName = string `dlq-${queueName}`;
            check self.deleteQueue(queueName);
            check self.deleteQueue(dlqName);
        }
    }

    isolated function retrieveQueue(string queueName) returns semp:MsgVpnQueue|error {
        string vpn = self.messageVpn;
        semp:MsgVpnQueueResponse|error response = self.administrator->getMsgVpnQueue(
            msgVpnName = vpn,
            queueName = queueName
        );
        if response is semp:MsgVpnQueueResponse {
            if response.data is semp:MsgVpnQueue {
                return <semp:MsgVpnQueue>response.data;
            }
            return error SolaceQueueNotFound(string `Empty response received when tried to retrieve queue [${queueName}] for vpn [${vpn}]`);
        }

        if response is http:ClientRequestError {
            http:Detail errorDetails = response.detail();
            if errorDetails.statusCode !== http:STATUS_BAD_REQUEST {
                return response;
            }
            record {semp:SempMeta meta;} payload = check errorDetails.body.cloneWithType();
            if "NOT_FOUND" === payload.meta.'error?.status {
                return error SolaceQueueNotFound(string `Could not find the queue [${queueName}] for vpn [${vpn}]`);
            }
            return response;
        }

        return response;
    }

    isolated function createQueue(string queueName, string? dlq = ()) returns semp:MsgVpnQueue|error {
        string vpn = self.messageVpn;
        semp:MsgVpnQueueResponse|error response = self.administrator->createMsgVpnQueue(msgVpnName = vpn, payload = {
            queueName,
            deadMsgQueue: dlq,
            accessType: "non-exclusive",
            permission: "delete",
            ingressEnabled: true,
            egressEnabled: true
        });
        if response is semp:MsgVpnQueueResponse {
            if response.data is semp:MsgVpnQueue {
                return <semp:MsgVpnQueue>response.data;
            }
            return error(string `Empty response received when tried to create a queue [${queueName}] in vpn [${vpn}]`);
        }

        if response is http:ClientRequestError {
            http:Detail errorDetails = response.detail();
            if errorDetails.statusCode !== http:STATUS_BAD_REQUEST {
                return response;
            }
            record {semp:SempMeta meta;} payload = check errorDetails.body.cloneWithType();
            if "NOT_FOUND" === payload.meta.'error?.status {
                return error(string `Could not find the vpn [${vpn}]`);
            }
            if "ALREADY_EXISTS" !== payload.meta.'error?.status {
                return response;
            }
            return error SolaceQueueExists(string `Queue [${queueName}] already exists in vpn [${vpn}]`);
        }
        return response;
    }

    isolated function addTopicSubscription(string queueName, string subscriptionTopic) returns semp:MsgVpnQueueSubscription|error {
        string vpn = self.messageVpn;
        semp:MsgVpnQueueSubscriptionResponse|error response = self.administrator->createMsgVpnQueueSubscription(
            msgVpnName = vpn,
            queueName = queueName,
            payload = {
            subscriptionTopic
        }
        );
        if response is semp:MsgVpnQueueSubscriptionResponse {
            if response.data is semp:MsgVpnQueueSubscription {
                return <semp:MsgVpnQueueSubscription>response.data;
            }
            return error(string `Empty response received when trying to add a topic subscription [${subscriptionTopic}] for a queue [${queueName}] in vpn [${vpn}]`);
        }

        if response is http:ClientRequestError {
            http:Detail errorDetails = response.detail();
            if errorDetails.statusCode !== http:STATUS_BAD_REQUEST {
                return response;
            }
            record {semp:SempMeta meta;} payload = check errorDetails.body.cloneWithType();
            if "NOT_FOUND" === payload.meta.'error?.status {
                return error(string `Could not find either the queue [${queueName}] or the vpn [${vpn}]`);
            }
            if "ALREADY_EXISTS" === payload.meta.'error?.status {
                return error api:SubscriptionExists(string `Topic subscription [${subscriptionTopic}] already existst for queue [${queueName}] in vpn [${vpn}]`);
            }
            return response;
        }
        return response;
    }

    isolated function retrieveTopicSubscriptions(string queueName) returns semp:MsgVpnQueueSubscription[]|error? {
        string vpn = self.messageVpn;
        semp:MsgVpnQueueSubscription[] subscriptions = [];
        string? cursor = ();

        while true {
            semp:MsgVpnQueueSubscriptionsResponse|error response;
            if cursor is string {
                response = self.administrator->getMsgVpnQueueSubscriptions(
                    msgVpnName = vpn,
                    queueName = queueName,
                    cursor = cursor
                );
            } else {
                response = self.administrator->getMsgVpnQueueSubscriptions(
                    msgVpnName = vpn,
                    queueName = queueName
                );
            }

            if response is http:ClientRequestError {
                http:Detail details = response.detail();
                if details.statusCode == http:STATUS_BAD_REQUEST {
                    record {semp:SempMeta meta;} payload = check details.body.cloneWithType();
                    if payload.meta.'error?.status == "NOT_FOUND" {
                        log:printDebug(string `No subscriptions found or queue [${queueName}] does not exist`);
                        // If the topic or VPN not found return nil, so that unsubscription could be successful when there is unexpected queue deletion
                        return;
                    }
                }
                return response;
            }
            if response is error {
                return response;
            }

            if response.data is semp:MsgVpnQueueSubscription[] {
                subscriptions.push(...(<semp:MsgVpnQueueSubscription[]>response.data));
            }

            semp:SempPaging? paging = response.meta.paging;
            if paging is () {
                break;
            }

            cursor = paging.cursorQuery;
        }

        return subscriptions;
    }

    isolated function removeTopicSubscription(string queueName, string subscriptionTopic) returns error? {
        _ = check self.administrator->deleteMsgVpnQueueSubscription(
            msgVpnName = self.messageVpn,
            queueName = queueName,
            subscriptionTopic = subscriptionTopic
        );
    }

    isolated function deleteQueue(string queueName) returns error? {
        var response = self.administrator->deleteMsgVpnQueue(
            msgVpnName = self.messageVpn,
            queueName = queueName
        );
        if response is http:ClientRequestError {
            http:Detail details = response.detail();
            if details.statusCode == http:STATUS_BAD_REQUEST {
                record {semp:SempMeta meta;} payload = check details.body.cloneWithType();
                if payload.meta.'error?.status == "NOT_FOUND" {
                    return;
                }
            }
            return response;
        }

        if response is error {
            return response;
        }
    }

    isolated remote function close() returns error? {
        return;
    }
}
