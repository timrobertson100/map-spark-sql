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

    // 20 mins
    // prepareInputDataToTile(spark, source, tilePyramidThreshold);

    spark.sql("DROP TABLE IF EXISTS map_input_tiles");
    spark
        .udf()
        .register("toTileXY", new TilePixelUDF(), DataTypes.createArrayType(DataTypes.StringType));
    spark.sparkContext().setJobDescription("Calculating tile coordinates");
    spark.sql(
        "CREATE TABLE map_input_tiles STORED AS parquet AS "
            + "SELECT mapKey, z0, z1, z2, z3, z4, z5, z6, z7, z8, z9, z10, year, bor, occCount "
            + "FROM map_input "
            + "LATERAL VIEW explode(toTileXY(0, lat, lng)) z AS z0 "
            + "LATERAL VIEW explode(toTileXY(1, lat, lng)) z AS z1 "
            + "LATERAL VIEW explode(toTileXY(2, lat, lng)) z AS z2 "
            + "LATERAL VIEW explode(toTileXY(3, lat, lng)) z AS z3 "
            + "LATERAL VIEW explode(toTileXY(4, lat, lng)) z AS z4 "
            + "LATERAL VIEW explode(toTileXY(5, lat, lng)) z AS z5 "
            + "LATERAL VIEW explode(toTileXY(6, lat, lng)) z AS z6 "
            + "LATERAL VIEW explode(toTileXY(7, lat, lng)) z AS z7 "
            + "LATERAL VIEW explode(toTileXY(8, lat, lng)) z AS z8 "
            + "LATERAL VIEW explode(toTileXY(9, lat, lng)) z AS z9 "
            + "LATERAL VIEW explode(toTileXY(10, lat, lng)) z AS z10");
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
            + "WHERE decimalLatitude BETWEEN -90 AND 90 "
            + "GROUP BY mapKey, lat, lng, bor, year");

    spark.sparkContext().setJobDescription("Generating counts per map type");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_type_totals STORED AS PARQUET AS "
            + "SELECT mapKey, count(*) AS total "
            + "FROM map_input "
            + "GROUP BY mapKey");

    spark
        .sparkContext()
        .setJobDescription(
            "Filtering input for views with record count >= " + tilePyramidThreshold);
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_input_to_tile STORED AS PARQUET AS "
            + "SELECT m.* "
            + "FROM map_input m JOIN map_type_totals t ON m.mapKey = t.mapKey "
            + "WHERE t.total >= "
            + tilePyramidThreshold);
  }
}
