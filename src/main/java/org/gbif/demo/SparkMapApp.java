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
import org.apache.spark.sql.types.*;

public class SparkMapApp {
  public static void main(String[] args) {

    String source = args.length == 2 ? args[0] : "prod_h.occurrence";
    String targetDB = args.length == 2 ? args[1] : "tim";
    int tilePyramidThreshold = 250000;

    SparkSession spark =
        SparkSession.builder().appName("SparkMapTest").enableHiveSupport().getOrCreate();
    SparkConf conf = spark.sparkContext().conf();

    conf.set("hive.exec.compress.output", "true"); // TODO: needed?
    spark.sql("use " + targetDB);

    registerUDFs(spark);
    // prepareInputDataToTile(spark, source, tilePyramidThreshold);
    for (int z = 16; z >= 0; z--) {
      processZoom(spark, z);
    }
  }

  private static void processZoom(SparkSession spark, int zoom) {
    spark.sparkContext().setJobDescription("Processing zoom " + zoom);

    spark.sql("DROP TABLE IF EXISTS map_projected_" + zoom);
    // SQL written and optimised for Spark 2.3 using available UDFs.
    // Further optimisations should be explored for Spark 3.x+ (hint: aggregation functions)
    spark.sql(
        String.format(
            "CREATE TABLE map_projected_%d STORED AS parquet AS "

                // groups the data onto tiles, with local addressing
                + "SELECT mapKey, tile, features "
                + "FROM ("

                //   Collects the counts at the global XY adding tile coordinates
                + "  SELECT mapKey, zxy, collect_list(borYearCount) as features "
                + "  FROM ("

                //     Filters the input, projects to globalXY and accumulates counts by bor+year
                + "    SELECT /*+ BROADCAST(map_stats) */ " // efficient threshold filtering
                + "      m.mapKey, "
                + "      project(%d, lat, lng) AS zxy, "
                + "      struct(m.borYear, sum(m.occCount) AS occCount) AS borYearCount "
                + "    FROM "
                + "      map_input m "
                + "      JOIN map_stats s ON m.mapKey = s.mapKey " // filters to maps above
                // threshold

                + "    GROUP BY m.mapKey, zxy, m.borYear "
                + "  ) t "
                + "GROUP BY mapKey, zxy"
                + ") f "
                + "LATERAL VIEW explode(collectToTiles(zxy.z, zxy.x, zxy.y)) t AS tile "
                + "WHERE zxy.z IS NOT NULL AND zxy.x IS NOT NULL AND zxy.y IS NOT NULL",
            zoom, zoom));
  }

  /**
   * Reads the occurrence data and generates a table of the map views with sufficient data that
   * should be prepared into the tile pyramid.
   */
  private static void prepareInputDataToTile(SparkSession spark, String source, int threshold) {
    spark.sql("DROP TABLE IF EXISTS map_input");
    spark.sql("DROP TABLE IF EXISTS map_stats");

    spark.sparkContext().setJobDescription("Reading input data");
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS map_input STORED AS parquet AS "
                + "SELECT "
                + "  mapKey, "
                + "  decimalLatitude AS lat, "
                + "  decimalLongitude AS lng, "
                + "  encodeBorYear(basisOfRecord, year) AS borYear, " // TODO: performance as a
                // struct()?
                + "  count(*) AS occCount "
                + "FROM "
                + "  %s "
                + "  LATERAL VIEW explode(  "
                + "    mapKeys(" // TODO: performance as a collect_set?
                + "      kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,"
                + "      datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey"
                + "    ) "
                + "  ) m AS mapKey "
                + "WHERE "
                + "  decimalLatitude BETWEEN -90 AND 90 AND "
                + "  decimalLongitude IS NOT NULL AND "
                + "  hasGeospatialIssues = false AND "
                + "  occurrenceStatus='PRESENT' " // AND phylumKey=35
                + "GROUP BY mapKey, lat, lng, borYear",
            source));

    // Broadcasting a stats table proves faster than a windowing function and is simpler to grok
    spark.sparkContext().setJobDescription("Creating input stats using threshold of " + threshold);
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS map_stats STORED AS PARQUET AS "
                + "SELECT mapKey, count(*) AS total "
                + "FROM map_input "
                + "GROUP BY mapKey "
                + "HAVING count(*) >= %d",
            threshold));
  }

  private static void registerUDFs(SparkSession spark) {
    spark
        .udf()
        .register("mapKeys", new MapKeysUDF(), DataTypes.createArrayType(DataTypes.StringType));
    spark.udf().register("encodeBorYear", new EncodeBorYearUDF(), DataTypes.IntegerType);
    spark
        .udf()
        .register(
            "project",
            new GlobalPixelUDF(),
            DataTypes.createStructType(
                new StructField[] {
                  DataTypes.createStructField("z", DataTypes.IntegerType, false),
                  DataTypes.createStructField("x", DataTypes.LongType, false),
                  DataTypes.createStructField("y", DataTypes.LongType, false)
                }));
    spark
        .udf()
        .register(
            "collectToTiles",
            new TileXYUDF(),
            DataTypes.createArrayType(
                DataTypes.createStructType(
                    new StructField[] {
                      // Designed to work until zoom 16; higher requires tileXY to be LongType
                      DataTypes.createStructField("tileX", DataTypes.IntegerType, false),
                      DataTypes.createStructField("tileY", DataTypes.IntegerType, false),
                      DataTypes.createStructField("pixelX", DataTypes.IntegerType, false),
                      DataTypes.createStructField("pixelY", DataTypes.IntegerType, false)
                    })));
  }
}
