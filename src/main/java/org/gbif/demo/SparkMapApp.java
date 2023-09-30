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
import org.apache.spark.sql.types.ArrayType;
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
    prepareInputDataToTile(spark, source, tilePyramidThreshold);

    processZoom(spark, 16);
  }

  private static void processZoom(SparkSession spark, int maxZoom) {
    spark.udf().register("encodeBorYear", new EncodeBorYearUDF(), DataTypes.IntegerType);
    StructType globalAddress =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("z", DataTypes.IntegerType, false),
              DataTypes.createStructField("x", DataTypes.LongType, false),
              DataTypes.createStructField("y", DataTypes.LongType, false)
            });
    spark.udf().register("project", new GlobalPixelUDF(), DataTypes.createArrayType(globalAddress));

    spark.sql("DROP TABLE IF EXISTS map_projected");
    spark.sql(
        "CREATE TABLE map_projected STORED AS parquet AS "
            + "SELECT mapKey, z, x, y, collect_list(map(borYear,occCount)) as data "
            + "FROM ("
            + "  SELECT mapKey, xy.z, xy.x, xy.y, encodeBorYear(bor,year) AS borYear, sum(occCount) AS occCount   "
            + "  FROM map_input_filtered "
            + "  LATERAL VIEW explode(project("
            + maxZoom
            + ", lat, lng)) p AS xy "
            + "  GROUP BY mapKey, z, x, y, borYear"
            + ") t "
            + "GROUP BY mapKey, z, x, y");
  }

  /**
   * Reads the occurrence data and generates a table of the map views with sufficient data that
   * should be prepared into the tile pyramid.
   */
  private static void prepareInputDataToTile(
      SparkSession spark, String source, int tilePyramidThreshold) {
    spark.sql("DROP TABLE IF EXISTS map_input");
    spark.sql("DROP TABLE IF EXISTS map_stats");
    spark.sql("DROP TABLE IF EXISTS map_input_filtered");
    ArrayType arrayOfStrings = DataTypes.createArrayType(DataTypes.StringType);
    spark.udf().register("mapKeys", new MapKeysUDF(), arrayOfStrings);

    spark
        .sparkContext()
        .setJobDescription("Filtering input views with threshold " + tilePyramidThreshold);
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
            + "    datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey"
            + "  ) "
            + ") m AS mapKey "
            + "WHERE decimalLatitude BETWEEN -90 AND 90 "
            + "GROUP BY mapKey, lat, lng, bor, year");

    // Generating a stats table proves faster than a windowing function and is simpler to grok
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_stats STORED AS PARQUET AS "
            + "SELECT mapKey, count(*) AS total "
            + "FROM map_input "
            + "GROUP BY mapKey "
            + "HAVING count(*) > "
            + tilePyramidThreshold);

    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_input_filtered STORED AS PARQUET AS "
            + "SELECT /*+ BROADCAST(map_stats) */ m.* "
            + "FROM map_input m JOIN map_stats s ON m.mapKey = s.mapKey");
  }
}
