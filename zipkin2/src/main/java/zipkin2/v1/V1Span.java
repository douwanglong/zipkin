/*
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.Nullable;

import static java.util.Collections.unmodifiableList;
import static zipkin2.internal.HexCodec.lowerHexToUnsignedLong;

/** V1 spans are different than v2 especially as annotations repeat. */
public final class V1Span {

  /** When non-zero, the trace containing this span uses 128-bit trace identifiers. */
  public long traceIdHigh() {
    return traceIdHigh;
  }

  /** lower 64-bits of the {@link Span#traceId()} */
  public long traceId() {
    return traceId;
  }

  /** Same as {@link zipkin2.Span#id()} except packed into a long. Zero means root span. */
  public long id() {
    return id;
  }

  /** Same as {@link zipkin2.Span#name()} */
  public String name() {
    return name;
  }

  /** The parent's {@link #id()} or zero if this the root span in a trace. */
  public long parentId() {
    return parentId;
  }

  /** Same as {@link Span#timestampAsLong()} */
  public long timestamp() {
    return timestamp;
  }

  /** Same as {@link Span#durationAsLong()} */
  public long duration() {
    return duration;
  }

  /**
   * Same as {@link Span#annotations()}, except each may be associated with {@link
   * Span#localEndpoint()}
   */
  public List<V1Annotation> annotations() {
    return annotations;
  }

  /**
   * {@link Span#tags()} are allocated to binary annotations with a {@link
   * V1BinaryAnnotation#stringValue()}. {@link Span#remoteEndpoint()} are allocated to those with a
   * true {@link V1BinaryAnnotation#booleanValue()}.
   */
  public List<V1BinaryAnnotation> binaryAnnotations() {
    return binaryAnnotations;
  }

  /** Same as {@link Span#debug()} */
  public Boolean debug() {
    return debug;
  }

  final long traceIdHigh, traceId, id;
  final String name;
  final long parentId, timestamp, duration;
  final List<V1Annotation> annotations;
  final List<V1BinaryAnnotation> binaryAnnotations;
  final Boolean debug;

  V1Span(Builder builder) {
    if (builder.traceId == 0L) throw new IllegalArgumentException("traceId == 0");
    if (builder.id == 0L) throw new IllegalArgumentException("id == 0");
    this.traceId = builder.traceId;
    this.traceIdHigh = builder.traceIdHigh;
    this.name = builder.name;
    this.id = builder.id;
    this.parentId = builder.parentId;
    this.timestamp = builder.timestamp;
    this.duration = builder.duration;
    if (builder.annotations != null) {
      Collections.sort(builder.annotations);
      this.annotations = unmodifiableList(new ArrayList<>(builder.annotations));
    } else {
      this.annotations = Collections.emptyList();
    }
    if (builder.binaryAnnotations != null) {
      this.binaryAnnotations = unmodifiableList(new ArrayList<>(builder.binaryAnnotations));
    } else {
      this.binaryAnnotations = Collections.emptyList();
    }
    this.debug = builder.debug;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    long traceIdHigh, traceId, parentId, id;
    String name;
    long timestamp, duration;
    ArrayList<V1Annotation> annotations;
    ArrayList<V1BinaryAnnotation> binaryAnnotations;
    Boolean debug;

    Builder() {}

    public Builder clear() {
      traceId = 0;
      traceIdHigh = 0;
      name = null;
      id = 0;
      parentId = 0;
      timestamp = 0;
      duration = 0;
      if (annotations != null) annotations.clear();
      if (binaryAnnotations != null) binaryAnnotations.clear();
      debug = null;
      return this;
    }

    /** @see V1Span#name() */
    public Builder name(String name) {
      this.name = name == null || name.isEmpty() ? null : name.toLowerCase(Locale.ROOT);
      return this;
    }

    /** Same as {@link Span.Builder#traceId(String)} */
    public Builder traceId(String traceId) {
      if (traceId == null) throw new NullPointerException("traceId == null");
      if (traceId.length() == 32) {
        traceIdHigh = lowerHexToUnsignedLong(traceId, 0);
      }
      this.traceId = lowerHexToUnsignedLong(traceId);
      return this;
    }

    /** @see V1Span#traceId() */
    public Builder traceId(long traceId) {
      this.traceId = traceId;
      return this;
    }

    /** @see V1Span#traceIdHigh() */
    public Builder traceIdHigh(long traceIdHigh) {
      this.traceIdHigh = traceIdHigh;
      return this;
    }

    /** @see V1Span#id() */
    public Builder id(long id) {
      this.id = id;
      return this;
    }

    /** Same as {@link Span.Builder#id(String)} */
    public Builder id(String id) {
      if (id == null) throw new NullPointerException("id == null");
      this.id = lowerHexToUnsignedLong(id);
      return this;
    }

    /** Same as {@link Span.Builder#parentId(String)} */
    public Builder parentId(String parentId) {
      this.parentId = parentId != null ? lowerHexToUnsignedLong(parentId) : 0L;
      return this;
    }

    /** @see V1Span#parentId() */
    public Builder parentId(long parentId) {
      this.parentId = parentId;
      return this;
    }

    /** @see V1Span#timestamp() */
    public Builder timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    /** @see V1Span#duration() */
    public Builder duration(long duration) {
      this.duration = duration;
      return this;
    }

    /** @see V1Span#annotations() */
    public Builder addAnnotation(long timestamp, String value, @Nullable Endpoint endpoint) {
      if (annotations == null) annotations = new ArrayList<>(4);
      annotations.add(new V1Annotation(timestamp, value, endpoint));
      return this;
    }

    /** Creates an address annotation, which is the same as {@link Span#remoteEndpoint()} */
    public Builder addBinaryAnnotation(String address, Endpoint endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      if (binaryAnnotations == null) binaryAnnotations = new ArrayList<>(4);
      binaryAnnotations.add(new V1BinaryAnnotation(address, null, true, endpoint));
      return this;
    }

    /**
     * Creates a tag annotation, which is the same as {@link Span#tags()} except duplicating the
     * endpoint.
     *
     * <p>A special case is when the key is "lc" and value is empty: This substitutes for the {@link
     * Span#localEndpoint()}.
     */
    public Builder addBinaryAnnotation(String key, String value, Endpoint endpoint) {
      if (value == null) throw new NullPointerException("value == null");
      if (binaryAnnotations == null) binaryAnnotations = new ArrayList<>(4);
      binaryAnnotations.add(new V1BinaryAnnotation(key, value, false, endpoint));
      return this;
    }

    /** @see V1Span#debug() */
    public Builder debug(@Nullable Boolean debug) {
      this.debug = debug;
      return this;
    }

    public V1Span build() {
      return new V1Span(this);
    }
  }
}
