/*
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
package org.gbif.demo;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

/**
 * Aggregates the provided data into a MVT. Experimental - our version of Spark doesn't support
 * this.
 */
public class VectorTile2UDAF extends Aggregator<VectorTile2UDAF.In, VectorTile2UDAF.Agg, String> {
  static final ObjectMapper mapper =
      new ObjectMapper(); // TODO: JSON for test - replace by the actual MVT

  @Override
  public Agg zero() {
    return new Agg();
  }

  @Override
  public Agg reduce(Agg agg, In in) {
    agg.getData().merge(in.asAggKey(), in.count, Long::sum);
    return agg;
  }

  @Override
  public Agg merge(Agg agg, Agg agg2) {
    for (Map.Entry<AggKey, Long> e : agg2.getData().entrySet()) {
      agg.getData().merge(e.getKey(), e.getValue(), Long::sum);
    }
    return agg;
  }

  @SneakyThrows
  @Override
  public String finish(Agg agg) {
    return mapper.writeValueAsString(agg.getData());
  }

  @Override
  public Encoder<Agg> bufferEncoder() {
    return Encoders.bean(Agg.class);
  }

  @Override
  public Encoder<String> outputEncoder() {
    return Encoders.STRING();
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class In implements Serializable {
    private String bor;
    private Long year;
    private Long count;

    AggKey asAggKey() {
      return new AggKey(bor, year);
    }
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Agg implements Serializable {
    private Map<AggKey, Long> data = Maps.newHashMap();
  }

  /** Encoding the bor and year simplifies aggregation by avoid nested maps */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class AggKey implements Serializable {
    private String bor;
    private Long year;
  }
}
