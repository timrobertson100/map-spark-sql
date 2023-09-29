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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;
import org.codehaus.jackson.map.ObjectMapper;

import lombok.SneakyThrows;
import scala.collection.JavaConversions;

/** Aggregates the provided data into a MVT. */
public class VectorTileUDAF extends UserDefinedAggregateFunction {
  static final ObjectMapper mapper =
      new ObjectMapper(); // TODO: JSON for test - replace by the actual MVT

  @Override
  public StructType inputSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("bor", DataTypes.StringType, false),
          DataTypes.createStructField("year", DataTypes.StringType, true),
          DataTypes.createStructField("count", DataTypes.LongType, false)
        });
  }

  @Override
  public StructType bufferSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField(
              "data",
              DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true),
              false)
        });
  }

  @Override
  public DataType dataType() {
    return DataTypes.StringType;
  }

  @Override
  public boolean deterministic() {
    return true;
  }

  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, new HashMap<String, Long>());
  }

  @Override
  public void update(MutableAggregationBuffer buffer, Row row) {
    String bor = row.getAs(0);
    String year = row.getAs(1);
    Long count = row.getAs(2);
    String k = bor + "|" + year;
    Map<String, Long> data = JavaConversions.mapAsJavaMap(buffer.getAs(0)); // immutable
    Map<String, Long> result = new HashMap<>(data); // mutable copy
    if (result.containsKey(k)) result.put(k, data.get(k) + count);
    else result.put(k, count);
    buffer.update(0, result);
  }

  @Override
  public void merge(MutableAggregationBuffer buffer, Row buffer2) {
    Map<String, Long> data = JavaConversions.mapAsJavaMap(buffer.getAs(0)); // immutable
    Map<String, Long> data2 = JavaConversions.mapAsJavaMap(buffer2.getAs(0)); // immutable
    Map<String, Long> result = new HashMap<>(data); // mutable copy
    for (Map.Entry<String, Long> e : data2.entrySet()) {
      if (result.containsKey(e.getKey()))
        result.put(e.getKey(), result.get(e.getKey()) + e.getValue());
      else result.put(e.getKey(), e.getValue());
    }
    buffer.update(0, result);
  }

  @SneakyThrows
  @Override
  public Object evaluate(Row buffer) {
    Map<String, Long> data = JavaConversions.mapAsJavaMap(buffer.getAs(0));
    return mapper.writeValueAsString(data);
  }
}
