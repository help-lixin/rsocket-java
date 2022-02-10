/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.local;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.test.TransportTest;
import java.time.Duration;
import java.util.UUID;

final class LocalResumableTransportTest implements TransportTest {

  public static void main(String[] args) {

    System.out.println(
        FrameHeaderCodec.frameType(
            Unpooled.copiedBuffer(
                ByteBufUtil.decodeHexDump("000000003800000000000007ef4a"))));
    System.out.println(
        FrameHeaderCodec.frameType(
            Unpooled.copiedBuffer(
                ByteBufUtil.decodeHexDump("000000002c0000000004"))));
  }

  private final TransportPair transportPair =
      new TransportPair<>(
          () -> "test-" + UUID.randomUUID(),
          (address, server, allocator) -> LocalClientTransport.create(address, allocator),
          (address, allocator) -> LocalServerTransport.create(address),
          false,
          true);

  @Override
  public Duration getTimeout() {
    return Duration.ofSeconds(10);
  }

  @Override
  public TransportPair getTransportPair() {
    return transportPair;
  }
}
