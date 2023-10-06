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

import org.gbif.demo.udf.*;
import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import lombok.AllArgsConstructor;
import scala.Tuple2;

@AllArgsConstructor
public class TilePyramid {
  private final String source;
  private final String hiveDB;
  private final String zkQuorum;
  private final String targetTable;
  private final int modulo;
  private final String targetDir;
  private final int threshold;
  private final int tileSize;
  private final int bufferSize;
  private final int maxZoom;

  public static void main(String[] args) throws IOException {
    TilePyramid driver =
        new TilePyramid(
            "prod_h.occurrence",
            "tim",
            "c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181",
            "tim",
            100,
            "/tmp/tim",
            250000,
            512,
            64,
            16);
    driver.run();
  }

  private void run() throws IOException {
    SparkSession spark =
        SparkSession.builder().appName("SparkMapTest").enableHiveSupport().getOrCreate();
    SparkConf conf = spark.sparkContext().conf();
    conf.set("hive.exec.compress.output", "true");
    spark.sql("use " + hiveDB);

    runProjection(spark, "EPSG:3857", null);
    runProjection(spark, "EPSG:4326", null);
    runProjection(spark, "EPSG:3575", "decimalLatitude>=-1"); // 100km buffer
    runProjection(spark, "EPSG:3031", "decimalLatitude<=1"); // 100km buffer
  }

  /** Runs the tile pyramid build for the projection */
  private void runProjection(SparkSession spark, String epsg, String filter) throws IOException {
    spark.sparkContext().setJobDescription("Reading input data for " + epsg);
    prepareInput(spark, filter);

    for (int z = maxZoom; z >= 0; z--) { // slowest first
      spark.sparkContext().setJobDescription("Processing zoom " + z);
      String dir = targetDir + epsg.replaceAll(":", "_") + "/z" + z;

      Dataset<Row> tileData = createTiles(spark, epsg, z);
      JavaPairRDD<String, byte[]> vectorTiles = generateMVTs(tileData);
      writeHFiles(vectorTiles, epsg);
    }
  }

  /**
   * Prepares the occurrence data for tiling. This determines the map keys (e.g. Taxon 1, Country
   * DE) for each record, and encodes the basisOfRecord and Year into an Integer to improve
   * performance of subsequent aggregation counts. A parquet table is re-created in Hive to aid
   * diagnostics and reduce computation on any task failure. An additional stats table is created to
   * optimise filtering of data. Optionally, a WHERE clause can be provided to improve performance
   * on some projections ("lat>0") or to speed up experiments.
   */
  private void prepareInput(SparkSession spark, String filter) {
    MapKeysUDF.register(spark, "mapKeys");
    EncodeBorYearUDF.register(spark, "encodeBorYear");

    String where = filter != null ? String.format(" AND %s ", filter) : "";
    spark.sql("DROP TABLE IF EXISTS map_input");
    spark.sql(
        String.format(
            "CREATE TABLE map_input STORED AS parquet AS "
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
                + "  decimalLatitude IS NOT NULL AND "
                + "  decimalLongitude IS NOT NULL AND "
                + "  hasGeospatialIssues = false AND "
                + "  occurrenceStatus='PRESENT' %s"
                + "GROUP BY mapKey, lat, lng, borYear",
            source, where));

    // Broadcasting a stats table proves faster than a windowing function and is simpler to grok
    spark.sparkContext().setJobDescription("Creating input stats using threshold of " + threshold);
    spark.sql("DROP TABLE IF EXISTS map_stats");
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS map_stats STORED AS PARQUET AS "
                + "SELECT mapKey, count(*) AS total "
                + "FROM map_input "
                + "GROUP BY mapKey "
                + "HAVING count(*) >= %d",
            threshold));
  }

  /**
   * Performs the aggregations of counts for each pixel in the tiles. This first filters input and
   * projects coordinates to the global XY space, then aggregates counts at the pixel, then groups
   * the pixels into tiles noting that a pixel can fall on a tile and in a buffer of an adjacent
   * tile, and then finally encodes the data into an MVT for the tile.
   */
  private Dataset<Row> createTiles(SparkSession spark, String epsg, int zoom) {

    // filter input and project to global pixel address
    GlobalPixelUDF.register(spark, "project", epsg, tileSize);
    Dataset<Row> t1 =
        spark.sql(
            String.format(
                "      SELECT "
                    + "  /*+ BROADCAST(map_stats) */ " // efficient threshold filtering
                    + "  m.mapKey, "
                    + "  project(%d, lat, lng) AS zxy, "
                    + "  struct(borYear AS borYear, sum(occCount) AS occCount) AS borYearCount "
                    + "FROM "
                    + "  map_input m "
                    + "  JOIN map_stats s ON m.mapKey = s.mapKey " // threshold filter
                    + "GROUP BY m.mapKey, zxy, borYear",
                zoom));
    t1.createOrReplaceTempView("t1");

    // collect counts into a feature at the global pixel address
    Dataset<Row> t2 =
        spark.sql(
            "SELECT mapKey, zxy, collect_list(borYearCount) as features"
                + "  FROM t1 "
                + "  WHERE zxy IS NOT NULL"
                + "  GROUP BY mapKey, zxy");
    t2.createOrReplaceTempView("t2");

    // readdress pixels onto tiles noting that addresses in buffer zones fall on multiple tiles
    ModulusSalt salter = new ModulusSalt(modulo);
    HBaseKeyUDF.register(spark, "hbaseKey", salter);
    TileXYUDF.register(spark, "collectToTiles", epsg, tileSize, bufferSize);
    Dataset<Row> t3 =
        spark.sql(
            "SELECT "
                + "    hbaseKey(mapKey, zxy.z, tile.tileX, tile.tileY) AS key,"
                + "    collect_list("
                + "      struct(tile.pixelX AS x, tile.pixelY AS y, features AS f)"
                + "    ) AS tile "
                + "  FROM "
                + "    t2 "
                + "    LATERAL VIEW explode("
                + "      collectToTiles(zxy.z, zxy.x, zxy.y)" // readdresses global pixels
                + "    ) t AS tile "
                + "  GROUP BY key");
    t3.createOrReplaceTempView("t3");
    return t3;
  }

  /**
   * Generates the Vector Tiles for the provided data. A UDF is avoided here as it proved slower due
   * to the unwrapping of the scala wrapper around the byte[].
   */
  private JavaPairRDD<String, byte[]> generateMVTs(Dataset<Row> source) {
    VectorTiles vectorTiles = new VectorTiles(tileSize, bufferSize);
    return source
        .toJavaRDD()
        .mapToPair(
            (PairFunction<Row, String, byte[]>)
                r -> {
                  String saltedKey = r.getString(0);
                  List<Row> tileData = r.getList(1);
                  byte[] mvt = vectorTiles.generate(tileData);
                  return new Tuple2<>(saltedKey, mvt);
                });
  }

  /**
   * Generates the HFiles containing the tiles and saves them in the provided directory. This
   * partitions the data using the modulus of the prefix salt to match the target regions, sorts
   * within the partitions and then creates the HFiles.
   */
  private void writeHFiles(JavaPairRDD<String, byte[]> mvts, String epsg) throws IOException {
    byte[] colFamily = Bytes.toBytes(epsg.replaceAll(":", "_"));
    byte[] col = Bytes.toBytes("tile");
    ModulusSalt salter = new ModulusSalt(modulo);
    mvts.repartitionAndSortWithinPartitions(new SaltPrefixPartitioner(salter.saltCharCount()))
        .mapToPair(
            (PairFunction<Tuple2<String, byte[]>, ImmutableBytesWritable, KeyValue>)
                kvp -> {
                  byte[] saltedRowKey = Bytes.toBytes(kvp._1);
                  byte[] mvt = kvp._2;
                  ImmutableBytesWritable key = new ImmutableBytesWritable(saltedRowKey);
                  KeyValue row = new KeyValue(saltedRowKey, colFamily, col, mvt);
                  return new Tuple2<>(key, row);
                })
        .saveAsNewAPIHadoopFile(
            targetDir,
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            hadoopConf());
  }

  private Configuration hadoopConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    conf.set(FileOutputFormat.COMPRESS, "true");
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
    Job job = new Job(conf, "Map tile build"); // name not actually used
    HTable table = new HTable(conf, targetTable);
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    return job.getConfiguration(); // job created a copy of the conf
  }

  /** Partitions by the salt prefix on the given key (which aligns to HBase regions). */
  private static class SaltPrefixPartitioner extends Partitioner {
    final int numPartitions;

    public SaltPrefixPartitioner(int saltLength) {
      numPartitions = new Double(Math.pow(10, saltLength)).intValue();
    }

    @Override
    public int getPartition(Object key) {
      return ModulusSalt.saltFrom(key.toString());
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }
  }
}
