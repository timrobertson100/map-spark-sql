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

    StructType globalAddress =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("z", DataTypes.IntegerType, false),
              DataTypes.createStructField("x", DataTypes.LongType, false),
              DataTypes.createStructField("y", DataTypes.LongType, false)
            });
    spark.udf().register("project", new GlobalPixelUDF(), globalAddress);

    // 600 cores 6.5 mins, 300 cores 15 mins (1 min for Bryophytes using phylumKey=35)
    // prepareInputDataToTile(spark, source, tilePyramidThreshold);
    for (int z = 0; z < 16; z++) {
      processZoom(spark, z, tilePyramidThreshold);
    }
  }

  private static void processZoom(SparkSession spark, int zoom, int threshold) {

    spark
        .sparkContext()
        .setJobDescription("Projecting data for zoom " + zoom + "  with threshold >= " + threshold);
    spark.sql("DROP TABLE IF EXISTS map_projected");
    spark.sql(
        "CREATE TABLE map_projected"
            + zoom
            + "  STORED AS parquet AS "
            + "SELECT "
            + "  mapKey, zxy.z, zxy.x, zxy.y, collect_list(map(borYear,occCount)) as data "
            + "FROM ("
            + "  SELECT "
            + "  /*+ BROADCAST(map_stats) */ " // efficient threshold filtering
            + "  m.mapKey, project("
            + zoom
            + ", lat, lng) AS zxy, m.borYear, sum(m.occCount) AS occCount   "
            + "  FROM map_input m "
            + "  JOIN map_stats s ON m.mapKey = s.mapKey " // filters to maps above threshold
            + "  GROUP BY m.mapKey, zxy, m.borYear"
            + ") t "
            + "GROUP BY mapKey, zxy.z, zxy.x, zxy.y");
  }

  /**
   * Reads the occurrence data and generates a table of the map views with sufficient data that
   * should be prepared into the tile pyramid.
   */
  private static void prepareInputDataToTile(SparkSession spark, String source, int threshold) {
    spark.sql("DROP TABLE IF EXISTS map_input");
    spark.sql("DROP TABLE IF EXISTS map_stats");
    ArrayType arrayOfStrings = DataTypes.createArrayType(DataTypes.StringType);
    spark.udf().register("mapKeys", new MapKeysUDF(), arrayOfStrings);

    spark.sparkContext().setJobDescription("Reading input data");
    spark.udf().register("encodeBorYear", new EncodeBorYearUDF(), DataTypes.IntegerType);
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_input STORED AS parquet AS "
            + "SELECT "
            + "  mapKey, "
            + "  decimalLatitude AS lat, "
            + "  decimalLongitude AS lng, "
            + "  encodeBorYear(basisOfRecord, year) AS borYear, "
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
            + "WHERE "
            + "  decimalLatitude BETWEEN -90 AND 90 AND "
            + "  decimalLongitude IS NOT NULL AND "
            + "  hasGeospatialIssues = false AND "
            + "  occurrenceStatus='PRESENT' " // AND phylumKey=35
            + "GROUP BY mapKey, lat, lng, borYear");

    // Generating and broadcasting a stats table proves much faster than a windowing function and is
    // simpler to grok
    spark.sparkContext().setJobDescription("Creating stats on input");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS map_stats STORED AS PARQUET AS "
            + "SELECT mapKey, count(*) AS total "
            + "FROM map_input "
            + "GROUP BY mapKey "
            + "HAVING count(*) > "
            + threshold);
  }
}
