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
package org.gbif.demo.udf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import lombok.AllArgsConstructor;
import no.ecc.vectortile.VectorTileEncoder;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/** Generates a vector tile from the structured input. */
@AllArgsConstructor
public class VectorTileUDF implements UDF1<Seq<Row>, byte[]>, Serializable {
  private int tileSize;
  private int bufferSize;
  private static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public static void register(SparkSession spark, String name, int tileSize, int bufferSize) {
    spark
        .udf()
        .register(
            name,
            new VectorTileUDF(tileSize, bufferSize),
            DataTypes.createArrayType(DataTypes.ByteType));
  }

  @Override
  public byte[] call(Seq<Row> tileData) {
    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);
    for (Row pixel : JavaConversions.asJavaCollection(tileData)) {
      int x = pixel.getAs("x");
      int y = pixel.getAs("y");
      int i = pixel.fieldIndex("f"); // defensive
      List<Row> features = pixel.getList(i);

      Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));

      // Restructures our encoded list of borYear:count to the target structure in the MVT
      Map<String, Map<String, Long>> target = new HashMap<>();
      for (Row encoded : features) {
        String bor = EncodeBorYearUDF.bor(encoded.getAs("borYear"));
        Integer year = EncodeBorYearUDF.year(encoded.getAs("borYear"));
        long count = encoded.getAs("occCount");
        Map<String, Long> yearCounts = target.getOrDefault(bor, new HashMap<>());
        yearCounts.put(String.valueOf(year), count); // TODO: what are nulls meant to be?
        if (!target.containsKey(bor)) target.put(bor, yearCounts);
      }

      for (String bor : target.keySet()) {
        encoder.addFeature(bor, target.get(bor), point);
      }
    }
    return encoder.encode();
  }
}
