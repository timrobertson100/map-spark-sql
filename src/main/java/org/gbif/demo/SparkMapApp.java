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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkMapApp {
  public static void main(String[] args) {

    String source = args.length == 2 ? args[0] : "prod_h.occurrence";
    String targetDB = args.length == 2 ? args[1] : "tim";
    int tilePyramidThreshold =
        250000; // only tile maps with more than 250000 records (TODO: or maybe unique coords?)

    SparkSession spark =
        SparkSession.builder().appName("SparkMapTest").enableHiveSupport().getOrCreate();
    SparkConf conf = spark.sparkContext().conf();

    conf.set("hive.exec.compress.output", "true");
    spark.sql("use " + targetDB);

    // 20 mins (1min for Bryophytes with phylumKey=35)
    // prepareInputDataToTile(spark, source, tilePyramidThreshold);

    StructType pixelAddress =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("tileX", DataTypes.LongType, false),
              DataTypes.createStructField("tileY", DataTypes.LongType, false),
              DataTypes.createStructField("pixelX", DataTypes.IntegerType, false),
              DataTypes.createStructField("pixelY", DataTypes.IntegerType, false)
            });

    spark.udf().register("toTileXY", new TilePixelUDF(), DataTypes.createArrayType(pixelAddress));
    spark.udf().register("toMVT", new VectorTileUDAF()); // deprecated in Spark 3
    for (int z = 16; z >= 0; z -= 1) {
      processZoom(spark, z);
    }
  }

  private static void processZoom(SparkSession spark, int zoom) {
    spark.sql("DROP TABLE IF EXISTS z" + zoom + "_map_input_tiles");
    spark.sparkContext().setJobDescription("Calculating tile coordinates for zoom " + zoom);
    String z = "z" + zoom;
    spark.sql(
        "CREATE TABLE "
            + z
            + "_map_input_tiles STORED AS parquet AS "
            + "SELECT mapKey, z.tileX, z.tileY, z.pixelX, z.pixelY, bor, year, sum(occCount) AS occCount "
            + "FROM map_input_to_tile "
            + "LATERAL VIEW explode(toTileXY("
            + zoom
            + ", lat, lng)) t AS z "
            + "GROUP BY mapKey, tileX, tileY, pixelX, pixelY, bor, year");

    spark.sql(
        "CREATE TABLE "
            + z
            + "_map_tiles STORED AS parquet AS "
            + "SELECT mapKey, tileX, tileY, pixelX, pixelY, toMVT(bor, year, occCount) AS data "
            + "FROM "
            + z
            + "_map_input_tiles "
            + "GROUP BY mapKey, tileX, tileY, pixelX, pixelY");
  }

  /**
   * Reads the occurrence data and generates a table of the map views with sufficient data that
   * should be prepared into the tile pyramid.
   */
  private static void prepareInputDataToTile(
      SparkSession spark, String source, int tilePyramidThreshold) {
    spark.sql("DROP TABLE IF EXISTS map_input");
    spark.sql("DROP TABLE IF EXISTS map_type_totals");
    spark.sql("DROP TABLE IF EXISTS map_input_to_tile");
    spark
        .udf()
        .register("mapKeys", new MapKeysUDF(), DataTypes.createArrayType(DataTypes.StringType));
    spark.sparkContext().setJobDescription("Reading occurrence data");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_input STORED AS parquet AS "
            + "SELECT "
            + "  mapKey, "
            + "  decimalLatitude AS lat, "
            + "  decimalLongitude AS lng, "
            + "  basisOfRecord AS bor, "
            + "  year, "
            + "  count(*) AS occCount "
            + "FROM "
            + source
            + " "
            + "LATERAL VIEW explode(  "
            + "  mapKeys("
            + "    kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,"
            + "    datasetKey, publishingOrgKey, countryCode,publishingCountry, networkKey"
            + "  ) "
            + ") m AS mapKey "
            + "WHERE decimalLatitude BETWEEN -90 AND 90 " // AND phylumKey=35 "
            + "GROUP BY mapKey, lat, lng, bor, year");

    // Generating a count table proves faster than the windowing function approach and is simpler to
    // grok
    spark.sparkContext().setJobDescription("Generating counts per map type");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_type_totals STORED AS PARQUET AS "
            + "SELECT mapKey, count(*) AS total "
            + "FROM map_input "
            + "GROUP BY mapKey");

    // Broadcast to avoid the long tail of skew has proven the fastest approach in testing.
    // A straight join has very large skew
    spark
        .sparkContext()
        .setJobDescription(
            "Filtering input for views with record count >= " + tilePyramidThreshold);
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_input_to_tile STORED AS PARQUET AS "
            + "SELECT /*+ BROADCAST(map_type_totals) */ m.* "
            + "FROM map_input m JOIN map_type_totals t ON m.mapKey = t.mapKey "
            + "WHERE t.total >= "
            + tilePyramidThreshold);
  }
}
