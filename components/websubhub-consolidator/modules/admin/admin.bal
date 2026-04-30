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

import websubhub.consolidator.config;

import ballerina/log;

import wso2/messagestore as store;
import wso2/messagestore.api as storeapi;

final storeapi:Administrator administrator = check store:createAdministrator(config:store);

isolated function init() returns error? {
    // Create topic and subscription for state snapshot consumer
    check createStateSnapshotSubscription();
    // Create topic and subscription for state events consumer
    check createStateEventsSubscription();
}

isolated function createStateSnapshotSubscription() returns error? {
    var {topic, consumerId} = config:state.snapshot;
    error? result = administrator->createSubscription(topic, consumerId);
    if result is storeapi:SubscriptionExists {
        log:printWarn(string `Subscription for Topic [${topic}] and Subscriber [${consumerId}] exists`);
        return;
    }
    return result;
}

isolated function createStateEventsSubscription() returns error? {
    var {topic, consumerId} = config:state.events;
    error? result = administrator->createSubscription(topic, consumerId);
    if result is storeapi:SubscriptionExists {
        log:printWarn(string `Subscription for Topic [${topic}] and Subscriber [${consumerId}] exists`);
        return;
    }
    return result;
}
