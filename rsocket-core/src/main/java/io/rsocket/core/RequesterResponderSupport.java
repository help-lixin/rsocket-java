package io.rsocket.core;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.plugins.RequestInterceptor;
import java.util.Objects;
import java.util.function.Function;
import reactor.util.annotation.Nullable;

class RequesterResponderSupport {

  private final int mtu;
  private final int maxFrameLength;
  private final int maxInboundPayloadSize;
  private final PayloadDecoder payloadDecoder;
  private final ByteBufAllocator allocator;
  private final DuplexConnection connection;
  @Nullable private final RequestInterceptor requestInterceptor;

  @Nullable final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<FrameHandler> activeStreams;

  boolean terminating;

  public RequesterResponderSupport(
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize,
      PayloadDecoder payloadDecoder,
      DuplexConnection connection,
      @Nullable StreamIdSupplier streamIdSupplier,
      Function<RSocket, ? extends RequestInterceptor> requestInterceptorFunction) {

    this.activeStreams = new IntObjectHashMap<>();
    this.mtu = mtu;
    this.maxFrameLength = maxFrameLength;
    this.maxInboundPayloadSize = maxInboundPayloadSize;
    this.payloadDecoder = payloadDecoder;
    this.allocator = connection.alloc();
    this.streamIdSupplier = streamIdSupplier;
    this.connection = connection;
    this.requestInterceptor = requestInterceptorFunction.apply((RSocket) this);
  }

  public int getMtu() {
    return mtu;
  }

  public int getMaxFrameLength() {
    return maxFrameLength;
  }

  public int getMaxInboundPayloadSize() {
    return maxInboundPayloadSize;
  }

  public PayloadDecoder getPayloadDecoder() {
    return payloadDecoder;
  }

  public ByteBufAllocator getAllocator() {
    return allocator;
  }

  public DuplexConnection getDuplexConnection() {
    return connection;
  }

  @Nullable
  public RequesterLeaseTracker getRequesterLeaseTracker() {
    return null;
  }

  @Nullable
  public RequestInterceptor getRequestInterceptor() {
    return requestInterceptor;
  }

  /**
   * Issues next {@code streamId}
   *
   * @return issued {@code streamId}
   * @throws RuntimeException if the {@link RequesterResponderSupport} is terminated for any reason
   */
  public int getNextStreamId() {
    final StreamIdSupplier streamIdSupplier = this.streamIdSupplier;
    if (streamIdSupplier != null) {
      synchronized (this) {
        if
        return streamIdSupplier.nextStreamId(this.activeStreams);
      }
    } else {
      throw new UnsupportedOperationException("Responder can not issue id");
    }
  }

  /**
   * Adds frameHandler and returns issued {@code streamId} back
   *
   * @param frameHandler to store
   * @return issued {@code streamId}
   * @throws RuntimeException if the {@link RequesterResponderSupport} is terminated for any reason
   */
  public int addAndGetNextStreamId(FrameHandler frameHandler) {
    final StreamIdSupplier streamIdSupplier = this.streamIdSupplier;
    if (streamIdSupplier != null) {
      final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
      synchronized (this) {
        final int streamId = streamIdSupplier.nextStreamId(activeStreams);

        activeStreams.put(streamId, frameHandler);

        return streamId;
      }
    } else {
      throw new UnsupportedOperationException("Responder can not issue id");
    }
  }

  public synchronized boolean add(int streamId, FrameHandler frameHandler) {
    if (this.terminating) {
      throw
    }
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    // copy of Map.putIfAbsent(key, value) without `streamId` boxing
    final FrameHandler previousHandler = activeStreams.get(streamId);
    if (previousHandler == null) {
      activeStreams.put(streamId, frameHandler);
      return true;
    }
    return false;
  }

  /**
   * Resolves {@link FrameHandler} by {@code streamId}
   *
   * @param streamId used to resolve {@link FrameHandler}
   * @return {@link FrameHandler} or {@code null}
   */
  @Nullable
  public synchronized FrameHandler get(int streamId) {
    return this.activeStreams.get(streamId);
  }

  /**
   * Removes {@link FrameHandler} if it is present and equals to the given one
   *
   * @param streamId to lookup for {@link FrameHandler}
   * @param frameHandler instance to check with the found one
   * @return {@code true} if there is {@link FrameHandler} for the given {@code streamId} and the
   *     instance equals to the passed one
   */
  public synchronized boolean remove(int streamId, FrameHandler frameHandler) {
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    // copy of Map.remove(key, value) without `streamId` boxing
    final FrameHandler curValue = activeStreams.get(streamId);
    if (!Objects.equals(curValue, frameHandler)) {
      return false;
    }
    activeStreams.remove(streamId);
    return true;
  }

  public synchronized void terminate() {
    this.terminating = true;
  }
}
