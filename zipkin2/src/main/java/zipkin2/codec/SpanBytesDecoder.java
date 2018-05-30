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
package zipkin2.codec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import zipkin2.Span;
import zipkin2.internal.JsonCodec;
import zipkin2.internal.Nullable;
import zipkin2.internal.Proto3Codec;
import zipkin2.internal.ThriftCodec;
import zipkin2.internal.V1JsonSpanReader;
import zipkin2.internal.V2SpanReader;
import zipkin2.v1.V1Span;
import zipkin2.v1.V1SpanConverter;

/** This is separate from {@link SpanBytesEncoder}, as it isn't needed for instrumentation */
@SuppressWarnings("ImmutableEnumChecker") // because span is immutable
public enum SpanBytesDecoder implements BytesDecoder<Span> {
  /** Corresponds to the Zipkin v1 json format */
  JSON_V1 {
    @Override
    public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override
    public boolean decode(byte[] bytes, Collection<Span> out) {
      Span result = decodeOne(bytes);
      if (result == null) return false;
      out.add(result);
      return true;
    }

    @Override
    public boolean decodeList(byte[] spans, Collection<Span> out) {
      List<V1Span> v1 = JsonCodec.readList(new V1JsonSpanReader(), spans);
      int length = v1.size();
      if (length == 0) return false;
      for (int i = 0; i < length; i++) {
        out.addAll(V1SpanConverter.convert(v1.get(i)));
      }
      return true;
    }

    @Override
    public Span decodeOne(byte[] span) {
      V1Span v1 = JsonCodec.readOne(new V1JsonSpanReader(), span);
      return V1SpanConverter.convert(v1).get(0);
    }

    @Override
    public List<Span> decodeList(byte[] spans) {
      List<V1Span> v1 = JsonCodec.readList(new V1JsonSpanReader(), spans);
      int length = v1.size();
      List<Span> result = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        result.addAll(V1SpanConverter.convert(v1.get(i)));
      }
      return result;
    }
  },
  /** Corresponds to the Zipkin v1 thrift format */
  THRIFT {
    @Override
    public Encoding encoding() {
      return Encoding.THRIFT;
    }

    @Override
    public boolean decode(byte[] span, Collection<Span> out) {
      return ThriftCodec.read(span, out);
    }

    @Override
    public boolean decodeList(byte[] spans, Collection<Span> out) {
      return ThriftCodec.readList(spans, out);
    }

    @Override
    public Span decodeOne(byte[] span) {
      return ThriftCodec.readOne(span);
    }

    @Override
    public List<Span> decodeList(byte[] spans) {
      return ThriftCodec.readList(spans);
    }
  },
  /** Corresponds to the Zipkin v2 json format */
  JSON_V2 {
    @Override
    public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override
    public boolean decode(byte[] span, Collection<Span> out) { // ex decode span in dependencies job
      return JsonCodec.read(new V2SpanReader(), span, out);
    }

    @Override
    public boolean decodeList(byte[] spans, Collection<Span> out) { // ex getTrace
      return JsonCodec.readList(new V2SpanReader(), spans, out);
    }

    /**
     * Visible for testing. This returns the first span parsed from the serialized object or null
     */
    @Override
    @Nullable
    public Span decodeOne(byte[] span) {
      return JsonCodec.readOne(new V2SpanReader(), span);
    }

    /** Convenience method for {@link #decode(byte[], Collection)} */
    @Override
    public List<Span> decodeList(byte[] spans) {
      return JsonCodec.readList(new V2SpanReader(), spans);
    }
  },
  PROTO3 {
    @Override
    public Encoding encoding() {
      return Encoding.PROTO3;
    }

    @Override
    public boolean decode(byte[] span, Collection<Span> out) { // ex decode span in dependencies job
      return Proto3Codec.read(span, out);
    }

    @Override
    public boolean decodeList(byte[] spans, Collection<Span> out) { // ex getTrace
      return Proto3Codec.readList(spans, out);
    }

    /**
     * Visible for testing. This returns the first span parsed from the serialized object or null
     */
    @Override
    @Nullable
    public Span decodeOne(byte[] span) {
      return Proto3Codec.readOne(span);
    }

    /** Convenience method for {@link #decode(byte[], Collection)} */
    @Override
    public List<Span> decodeList(byte[] spans) {
      return Proto3Codec.readList(spans);
    }
  };
}
