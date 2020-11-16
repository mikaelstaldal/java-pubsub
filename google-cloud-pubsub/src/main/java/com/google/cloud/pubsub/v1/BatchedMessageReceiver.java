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

import com.google.pubsub.v1.PubsubMessage;
import java.util.List;

/**
 * This interface can be implemented by users of {@link Subscriber} to receive messages in batches.
 */
public interface BatchedMessageReceiver {
  /**
   * Called when messages are received by the subscriber. The implementation must arrange for {@link
   * AckReplyConsumer#ack()} or {@link AckReplyConsumer#nack()} to be called after processing the
   * {@code message}s.
   */
  void receiveMessages(final List<PubsubMessage> messages, final AckReplyConsumer consumer);
}
