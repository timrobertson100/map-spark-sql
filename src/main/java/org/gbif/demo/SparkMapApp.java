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

import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import no.ecc.vectortile.VectorTileEncoder;
import scala.Tuple2;

public class SparkMapApp {
  public static void main(String[] args) throws IOException {

    String source = args.length == 2 ? args[0] : "prod_h.occurrence";
    String targetDB = args.length == 2 ? args[1] : "tim";
    int tilePyramidThreshold = 250000;
    ModulusSalt salter = new ModulusSalt(100);

    int bufferSize = 64;
    int tileSize = 512;

    SparkSession spark =
        SparkSession.builder().appName("SparkMapTest").enableHiveSupport().getOrCreate();
    SparkConf conf = spark.sparkContext().conf();

    conf.set("hive.exec.compress.output", "true"); // TODO: needed?
    spark.sql("use " + targetDB);

    registerUDFs(spark, salter);
    // prepareInputDataToTile(spark, source, tilePyramidThreshold);
    for (int z = 16; z >= 0; z--) {
      processZoom(spark, z, tileSize, bufferSize, salter);
    }
  }

  private static void processZoom(
      SparkSession spark, int zoom, int tileSize, int bufferSize, ModulusSalt salter)
      throws IOException {
    spark.sparkContext().setJobDescription("Processing zoom " + zoom);

    spark.sql("DROP TABLE IF EXISTS map_projected_" + zoom);
    // SQL written and optimised for Spark 2.3 using available UDFs.
    // Further optimisations should be explored for Spark 3.x+ (hint: aggregation functions)
    Dataset<Row> tiles =
        spark.sql(
            String.format(
                // "CREATE TABLE map_projected_%d STORED AS parquet AS "

                // Collect into tiles with local addressing
                "SELECT "
                    + "  hbaseKey(mapKey, zxy.z, tile.tileX, tile.tileY) AS key,"
                    + "  collect_list("
                    + "      struct(tile.pixelX AS x, tile.pixelY AS y, features AS f)"
                    + "  ) AS tile "
                    + "FROM ("
                    //   Collects the counts into a feature collection at the global pixel
                    + "  SELECT mapKey, zxy, collect_list(borYearCount) as features "
                    + "  FROM ("
                    //     Filter, project to globalXY and count at pixel by bor+year
                    + "    SELECT "
                    + "      /*+ BROADCAST(map_stats) */ " // efficient threshold filtering
                    + "      m.mapKey, "
                    + "      project(%d, lat, lng) AS zxy, "
                    + "      struct(m.borYear AS borYear, sum(m.occCount) AS occCount) AS borYearCount "
                    + "    FROM "
                    + "      map_input m "
                    + "      JOIN map_stats s ON m.mapKey = s.mapKey " // threshold filter
                    + "    GROUP BY m.mapKey, zxy, borYear "
                    + "  ) t "
                    + "  WHERE zxy IS NOT NULL "
                    + "  GROUP BY mapKey, zxy"
                    + ") f "
                    + "LATERAL VIEW explode(collectToTiles(zxy.z, zxy.x, zxy.y)) t AS tile "
                    + "GROUP BY key",
                // + "WHERE zxy.z IS NOT NULL AND zxy.x IS NOT NULL AND zxy.y IS NOT NULL",
                zoom));

    // Create the vector tiles, and prepare partitions for HFiles
    GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    Partitioner partitionBySalt = new SaltPrefixPartitioner(salter.saltCharCount());
    JavaPairRDD<String, byte[]> partitioned =
        tiles
            .toJavaRDD()
            .mapToPair(
                (PairFunction<Row, String, byte[]>)
                    r -> {
                      VectorTileEncoder encoder =
                          new VectorTileEncoder(tileSize, bufferSize, false);
                      String saltedKey = r.getString(0);
                      List<Row> tileData = r.getList(1);
                      for (Row pixel : tileData) {
                        int x = pixel.getAs("x");
                        int y = pixel.getAs("y");
                        int i = pixel.fieldIndex("f"); // TODO: just use 2?
                        List<Row> features = pixel.getList(i);

                        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));

                        Map<String, Map<String, Long>> target = new HashMap<>();
                        for (Row encoded : features) {
                          String bor = EncodeBorYearUDF.bor(encoded.getAs("borYear"));
                          int year = EncodeBorYearUDF.year(encoded.getAs("borYear"));
                          long count = encoded.getAs("occCount");
                          Map<String, Long> yearCounts = target.getOrDefault(bor, new HashMap<>());
                          yearCounts.put(String.valueOf(year), count);
                          if (!target.containsKey(bor)) target.put(bor, yearCounts);
                        }

                        for (String bor : target.keySet()) {
                          encoder.addFeature(bor, target.get(bor), point);
                        }
                      }

                      byte[] mvt = encoder.encode();
                      return new Tuple2<>(saltedKey, mvt);
                    })
            .repartitionAndSortWithinPartitions(partitionBySalt);

    Configuration conf = HBaseConfiguration.create();
    conf.set(
        "hbase.zookeeper.quorum", "c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181");
    // NOTE: job creates a copy of the conf
    Job job = new Job(conf, "Map tile build"); // name not actually used since we don't submit MR
    HTable table = new HTable(conf, "tim");
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    conf = job.getConfiguration();

    partitioned
        .mapToPair(
            (PairFunction<Tuple2<String, byte[]>, ImmutableBytesWritable, KeyValue>)
                kvp -> {
                  byte[] saltedRowKey = Bytes.toBytes(kvp._1);
                  byte[] mvt = kvp._2;

                  ImmutableBytesWritable key = new ImmutableBytesWritable(saltedRowKey);
                  // TODO projection support
                  KeyValue row =
                      new KeyValue(
                          saltedRowKey,
                          Bytes.toBytes(
                              "EPSG:3857".replaceAll(":", "_")), // column family (e.g. epsg_4326)
                          Bytes.toBytes("tile"),
                          mvt);
                  return new Tuple2<>(key, row);
                })
        .saveAsNewAPIHadoopFile(
            "/tmp/tim/EPSG_3857/z" + zoom,
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            conf);
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
                + "  encodeBorYear(basisOfRecord, year) AS borYear, " // improves performance
                + "  count(*) AS occCount "
                + "FROM "
                + "  %s "
                + "  LATERAL VIEW explode(  "
                + "    mapKeys("
                + "      kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,"
                + "      datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey"
                + "    ) "
                + "  ) m AS mapKey "
                + "WHERE "
                + "  decimalLatitude BETWEEN -90 AND 90 AND "
                + "  decimalLongitude IS NOT NULL AND "
                + "  hasGeospatialIssues = false AND "
                + "  occurrenceStatus='PRESENT' AND phylumKey=35 "
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

  private static void registerUDFs(SparkSession spark, ModulusSalt salter) {
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

    spark.udf().register("hbaseKey", new HBaseKeyUDF(salter), DataTypes.StringType);
  }
}
