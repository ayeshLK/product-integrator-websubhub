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

import websubhub.common;
import websubhub.config;
import websubhub.persistence as persist;
import websubhub.security;

import ballerina/http;
import ballerina/websubhub;

http:Service healthCheckService = service object {
    resource function get .() returns http:Ok {
        return {
            body: {
                "status": "active"
            }
        };
    }
};

websubhub:Service hubService = @websubhub:ServiceConfig {
    webHookConfig: {
        retryConfig: config:delivery.'retry
    }
} service object {

    # Registers a `topic` in the hub.
    #
    # + message - Details related to the topic-registration
    # + headers - `http:Headers` of the original `http:Request`
    # + return - `websubhub:TopicRegistrationSuccess` if topic registration is successful, `websubhub:TopicRegistrationError`
    # if topic registration failed or `error` if there is any unexpected error
    isolated remote function onRegisterTopic(websubhub:TopicRegistration message, http:Headers headers)
                                returns websubhub:TopicRegistrationSuccess|websubhub:TopicRegistrationError|error {
        if config:securityOn {
            check security:authorize(headers, ["register_topic"]);
        }
        check self.registerTopic(message);
        return websubhub:TOPIC_REGISTRATION_SUCCESS;
    }

    isolated function registerTopic(websubhub:TopicRegistration message) returns websubhub:TopicRegistrationError? {
        lock {
            if registeredTopicsCache.hasKey(message.topic) {
                return error websubhub:TopicRegistrationError(
                    string `Topic [${message.topic}] is already registered with the Hub`, statusCode = http:STATUS_CONFLICT);
            }
            error? persistingResult = persist:addRegsiteredTopic(message.cloneReadOnly());
            if persistingResult is error {
                common:logError("Error occurred while persisting the topic-registration", persistingResult);
            }
        }
    }

    # Deregisters a `topic` in the hub.
    #
    # + message - Details related to the topic-deregistration
    # + headers - `http:Headers` of the original `http:Request`
    # + return - `websubhub:TopicDeregistrationSuccess` if topic deregistration is successful, `websubhub:TopicDeregistrationError`
    # if topic deregistration failed or `error` if there is any unexpected error
    isolated remote function onDeregisterTopic(websubhub:TopicDeregistration message, http:Headers headers)
                        returns websubhub:TopicDeregistrationSuccess|websubhub:TopicDeregistrationError|error {
        if config:securityOn {
            check security:authorize(headers, ["deregister_topic"]);
        }
        check self.deregisterTopic(message);
        return websubhub:TOPIC_DEREGISTRATION_SUCCESS;
    }

    isolated function deregisterTopic(websubhub:TopicRegistration message) returns websubhub:TopicDeregistrationError? {
        lock {
            if !registeredTopicsCache.hasKey(message.topic) {
                return error websubhub:TopicDeregistrationError(
                    string `Topic [${message.topic}] is not registered with the Hub`, statusCode = http:STATUS_NOT_FOUND);
            }
            error? persistingResult = persist:removeRegsiteredTopic(message.cloneReadOnly());
            if persistingResult is error {
                common:logError("Error occurred while persisting the topic-deregistration", persistingResult);
            }
        }
    }

    # Subscribes a `subscriber` to the hub.
    #
    # + message - Details of the subscription
    # + headers - `http:Headers` of the original `http:Request`
    # + return - `websubhub:SubscriptionAccepted` if subscription is accepted from the hub, `websubhub:BadSubscriptionError`
    # if subscription is denied from the hub or `error` if there is any unexpected error
    isolated remote function onSubscription(websubhub:Subscription message, http:Headers headers)
                returns websubhub:SubscriptionAccepted|websubhub:BadSubscriptionError|error {
        if config:securityOn {
            check security:authorize(headers, ["subscribe"]);
        }
        return websubhub:SUBSCRIPTION_ACCEPTED;
    }

    # Validates a incomming subscription request.
    #
    # + message - Details of the subscription
    # + return - `websubhub:SubscriptionDeniedError` if the subscription is denied by the hub or else `()`
    isolated remote function onSubscriptionValidation(websubhub:Subscription message)
                returns websubhub:SubscriptionDeniedError? {
        boolean topicAvailable = false;
        lock {
            topicAvailable = registeredTopicsCache.hasKey(message.hubTopic);
        }
        if !topicAvailable {
            return error websubhub:SubscriptionDeniedError(
                string `Topic [${message.hubTopic}] is not registered with the Hub`, statusCode = http:STATUS_NOT_ACCEPTABLE);
        } else {
            string subscriberId = common:generateSubscriberId(message.hubTopic, message.hubCallback);
            websubhub:VerifiedSubscription? subscription = getSubscription(subscriberId);
            if subscription is () {
                return;
            }
            if subscription.hasKey(STATUS) && subscription.get(STATUS) is STALE_STATE {
                return;
            }
            if isValidSubscription(subscriberId) {
                return error websubhub:SubscriptionDeniedError(
                    string `Active subscription for Topic [${message.hubTopic}] and Callback [${message.hubCallback}] already exists`,
                    statusCode = http:STATUS_NOT_ACCEPTABLE
                );
            }
        }
    }

    # Processes a verified subscription request.
    #
    # + message - Details of the subscription
    # + return - `error` if there is any unexpected error or else `()`
    isolated remote function onSubscriptionIntentVerified(websubhub:VerifiedSubscription message) returns error? {
        websubhub:VerifiedSubscription subscription = self.prepareSubscriptionToBePersisted(message);
        error? persistingResult = persist:addSubscription(subscription);
        if persistingResult is error {
            common:logError("Error occurred while persisting the subscription", persistingResult);
        }
    }

    isolated function prepareSubscriptionToBePersisted(websubhub:VerifiedSubscription message) returns websubhub:VerifiedSubscription {
        string subscriberId = common:generateSubscriberId(message.hubTopic, message.hubCallback);
        websubhub:VerifiedSubscription? subscription = getSubscription(subscriberId);
        // if we have a stale subscription, remove the `status` flag from the subscription and persist it again
        if subscription is websubhub:Subscription {
            websubhub:VerifiedSubscription updatedSubscription = {
                ...subscription
            };
            _ = updatedSubscription.removeIfHasKey(STATUS);
            return updatedSubscription;
        }
        if !message.hasKey(CONSUMER_GROUP) {
            string consumerGroup = common:generateGroupName(message.hubTopic, message.hubCallback);
            message[CONSUMER_GROUP] = consumerGroup;
        }
        message[SERVER_ID] = config:server.id;
        return message;
    }

    # Unsubscribes a `subscriber` from the hub.
    #
    # + message - Details of the unsubscription
    # + headers - `http:Headers` of the original `http:Request`
    # + return - `websubhub:UnsubscriptionAccepted` if unsubscription is accepted from the hub, `websubhub:BadUnsubscriptionError`
    # if unsubscription is denied from the hub or `error` if there is any unexpected error
    isolated remote function onUnsubscription(websubhub:Unsubscription message, http:Headers headers)
                returns websubhub:UnsubscriptionAccepted|websubhub:BadUnsubscriptionError|error {
        if config:securityOn {
            check security:authorize(headers, ["subscribe"]);
        }
        return websubhub:UNSUBSCRIPTION_ACCEPTED;
    }

    # Validates a incomming unsubscription request.
    #
    # + message - Details of the unsubscription
    # + return - `websubhub:UnsubscriptionDeniedError` if the unsubscription is denied by the hub or else `()`
    isolated remote function onUnsubscriptionValidation(websubhub:Unsubscription message)
                returns websubhub:UnsubscriptionDeniedError? {
        boolean topicAvailable = false;
        lock {
            topicAvailable = registeredTopicsCache.hasKey(message.hubTopic);
        }
        if !topicAvailable {
            return error websubhub:UnsubscriptionDeniedError(
                string `Topic [${message.hubTopic}] is not registered with the Hub`, statusCode = http:STATUS_NOT_ACCEPTABLE);
        } else {
            string subscriberId = common:generateSubscriberId(message.hubTopic, message.hubCallback);
            if !isValidSubscription(subscriberId) {
                return error websubhub:UnsubscriptionDeniedError(
                    string `Could not find a valid subscription for Topic [${message.hubTopic}] and Callback [${message.hubCallback}]`,
                    statusCode = http:STATUS_NOT_ACCEPTABLE
                );
            }
        }
    }

    # Processes a verified unsubscription request.
    #
    # + message - Details of the unsubscription
    isolated remote function onUnsubscriptionIntentVerified(websubhub:VerifiedUnsubscription message) {
        lock {
            error? persistingResult = persist:removeSubscription(message.cloneReadOnly());
            if persistingResult is error {
                common:logError("Error occurred while persisting the unsubscription", persistingResult);
            }
        }
    }

    # Publishes content to the hub.
    #
    # + message - Details of the published content
    # + headers - `http:Headers` of the original `http:Request`
    # + return - `websubhub:Acknowledgement` if publish content is successful, `websubhub:UpdateMessageError`
    # if publish content failed or `error` if there is any unexpected error
    isolated remote function onUpdateMessage(websubhub:UpdateMessage message, http:Headers headers)
                returns websubhub:Acknowledgement|websubhub:UpdateMessageError|error {
        if config:securityOn {
            check security:authorize(headers, ["update_content"]);
        }
        check self.updateMessage(message, headers);
        return websubhub:ACKNOWLEDGEMENT;
    }

    isolated function updateMessage(websubhub:UpdateMessage message, http:Headers headers) returns websubhub:UpdateMessageError? {
        boolean topicAvailable = false;
        lock {
            topicAvailable = registeredTopicsCache.hasKey(message.hubTopic);
        }
        if topicAvailable {
            map<string[]> messageHeaders = getHeadersMap(headers);
            error? errorResponse = persist:addUpdateMessage(message.hubTopic, message, messageHeaders);
            if errorResponse is websubhub:UpdateMessageError {
                return errorResponse;
            } else if errorResponse is error {
                common:logError("Error occurred while publishing the content", errorResponse);
                return error websubhub:UpdateMessageError(
                    errorResponse.message(), statusCode = http:STATUS_INTERNAL_SERVER_ERROR);
            }
        } else {
            return error websubhub:UpdateMessageError(
                string `Topic [${message.hubTopic}] is not registered with the Hub`, statusCode = http:STATUS_NOT_FOUND);
        }
    }
};

isolated function getHeadersMap(http:Headers httpHeaders) returns map<string[]> {
    map<string[]> headers = {};
    foreach string headerName in httpHeaders.getHeaderNames() {
        var headerValues = httpHeaders.getHeaders(headerName);
        // safe to ingore the error as here we are retrieving only the available headers
        if headerValues is error {
            continue;
        }
        headers[headerName] = headerValues;
    }
    return headers;
}
