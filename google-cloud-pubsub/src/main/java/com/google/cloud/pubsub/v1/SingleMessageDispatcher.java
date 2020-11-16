/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsub.v1;

import com.google.api.core.ApiClock;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.Distribution;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.threeten.bp.Duration;

class SingleMessageDispatcher extends MessageDispatcher {

  private final MessageReceiver receiver;

  SingleMessageDispatcher(
      MessageReceiver receiver,
      AckProcessor ackProcessor,
      Duration ackExpirationPadding,
      Duration maxAckExtensionPeriod,
      Duration maxDurationPerAckExtension,
      Distribution ackLatencyDistribution,
      FlowController flowController,
      Executor executor,
      ScheduledExecutorService systemExecutor,
      ApiClock clock) {
    super(
        ackProcessor,
        ackExpirationPadding,
        maxAckExtensionPeriod,
        maxDurationPerAckExtension,
        ackLatencyDistribution,
        flowController,
        executor,
        systemExecutor,
        clock);
    this.receiver = receiver;
  }

  @Override
  void processBatch(List<OutstandingMessage> batch) {
    messagesWaiter.incrementPendingCount(batch.size());
    for (OutstandingMessage message : batch) {
      // This is a blocking flow controller.  We have already incremented messagesWaiter, so
      // shutdown will block on processing of all these messages anyway.
      try {
        flowController.reserve(1, message.receivedMessage.getMessage().getSerializedSize());
      } catch (FlowController.FlowControlException unexpectedException) {
        // This should be a blocking flow controller and never throw an exception.
        throw new IllegalStateException("Flow control unexpected exception", unexpectedException);
      }
      processOutstandingMessage(addDeliveryInfoCount(message.receivedMessage), message.ackHandler);
    }
  }

  private void processOutstandingMessage(final PubsubMessage message, final AckHandler ackHandler) {
    final SettableApiFuture<AckReply> response = SettableApiFuture.create();
    final AckReplyConsumer consumer =
        new AckReplyConsumer() {
          @Override
          public void ack() {
            response.set(AckReply.ACK);
          }

          @Override
          public void nack() {
            response.set(AckReply.NACK);
          }
        };
    ApiFutures.addCallback(response, ackHandler, MoreExecutors.directExecutor());
    Runnable deliverMessageTask =
        new Runnable() {
          @Override
          public void run() {
            try {
              if (ackHandler
                  .totalExpiration
                  .plusSeconds(messageDeadlineSeconds.get())
                  .isBefore(now())) {
                // Message expired while waiting. We don't extend these messages anymore,
                // so it was probably sent to someone else. Don't work on it.
                // Don't nack it either, because we'd be nacking someone else's message.
                ackHandler.forget();
                return;
              }

              receiver.receiveMessage(message, consumer);
            } catch (Exception e) {
              response.setException(e);
            }
          }
        };
    if (message.getOrderingKey().isEmpty()) {
      executor.execute(deliverMessageTask);
    } else {
      sequentialExecutor.submit(message.getOrderingKey(), deliverMessageTask);
    }
  }
}
