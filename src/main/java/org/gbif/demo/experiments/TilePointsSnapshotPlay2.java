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
package org.gbif.demo.experiments;

import org.gbif.demo.ProtobufTiles;
import org.gbif.demo.SaltPrefixPartitioner;
import org.gbif.demo.udf.EncodeBorYearUDF;
import org.gbif.demo.udf.HBaseKeyUDF;
import org.gbif.demo.udf.MapKeysUDF;
import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;

import lombok.AllArgsConstructor;
import scala.Tuple2;

@AllArgsConstructor
public class TilePointsSnapshotPlay2 implements Serializable {
  private final String source;
  private final String hiveDB;
  private final String zkQuorum;
  private final String targetTable;
  private final int modulo;
  private final String targetDir;
  private final int threshold;

  public static void main(String[] args) throws IOException {
    TilePointsSnapshotPlay2 driver =
        new TilePointsSnapshotPlay2(
            "prod_h.occurrence",
            "tim",
            "c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181",
            "tim",
            100,
            "/tmp/tim-points",
            250000);
    driver.run();
  }

  private void run() throws IOException {
    SparkSession spark =
        SparkSession.builder().appName("Map Points").enableHiveSupport().getOrCreate();
    SparkConf conf = spark.sparkContext().conf();
    conf.set("hive.exec.compress.output", "true");
    spark.sql("use " + hiveDB);

    Set<String> mapKeys = prepareInput(spark);

    MapKeysUDF.register(spark, "mapKeys", mapKeys, false);
    HBaseKeyUDF.registerPointKey(spark, "hbaseKey", new ModulusSalt(modulo));
    EncodeBorYearUDF.register(spark, "encodeBorYear");
    Dataset<Row> t1 =
        spark.sql(
            "SELECT "
                + "    hbaseKey(mapKey) AS mapKey, "
                + "    decimalLatitude AS lat, "
                + "    decimalLongitude AS lng, "
                + "    encodeBorYear(basisOfRecord, year) AS borYear, "
                + "    count(*) AS occCount "
                + "  FROM "
                + "    point_map_input m "
                + "    LATERAL VIEW explode(  "
                + "      mapKeys("
                + "        kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,"
                + "        datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey"
                + "      ) "
                + "    ) m AS mapKey "
                + "  GROUP BY mapKey, lat, lng, borYear");
    t1.createOrReplaceTempView("t1");

    Dataset<Row> t2 =
        spark.sql(
            "SELECT "
                + "    mapKey, "
                + "    collect_list(struct(lat, lng, borYear, occCount)) AS features"
                + "  FROM t1"
                + "  GROUP BY mapKey");

    JavaPairRDD<String, byte[]> t3 =
        t2.javaRDD()
            .mapToPair(
                row -> {
                  String saltedKey = row.getString(0);
                  byte[] pb = ProtobufTiles.generate(row);
                  return new Tuple2<>(saltedKey, pb);
                });

    ModulusSalt salter = new ModulusSalt(modulo);
    t3.repartitionAndSortWithinPartitions(new SaltPrefixPartitioner(salter.saltCharCount()))
        .mapToPair(
            (PairFunction<Tuple2<String, byte[]>, ImmutableBytesWritable, KeyValue>)
                kvp -> {
                  byte[] saltedRowKey = Bytes.toBytes(kvp._1);
                  byte[] tile = kvp._2;
                  ImmutableBytesWritable key = new ImmutableBytesWritable(saltedRowKey);
                  KeyValue row =
                      new KeyValue(
                          saltedRowKey,
                          Bytes.toBytes("EPSG_4326"),
                          Bytes.toBytes("features"),
                          tile);
                  return new Tuple2<>(key, row);
                })
        .saveAsNewAPIHadoopFile(
            targetDir,
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            hadoopConf());
  }

  /**
   * Prepares the occurrence data. This determines the map keys (e.g. Taxon 1, Country DE) for each
   * record, and encodes the basisOfRecord and Year into an Integer to improve performance of
   * subsequent aggregation counts. A parquet table is re-created in Hive to aid diagnostics and
   * reduce computation on any task failure. An additional stats table is created to optimise
   * filtering of data.
   */
  private Set<String> prepareInput(SparkSession spark) {
    Dataset<Row> source =
        spark
            .read()
            .format("com.databricks.spark.avro")
            // .load("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/fff*.avro")
            .load("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/*.avro")
            .select(
                "datasetKey",
                "publishingOrgKey",
                "publishingCountry",
                "networkKey",
                "countryCode",
                "basisOfRecord",
                "decimalLatitude",
                "decimalLongitude",
                "kingdomKey",
                "phylumKey",
                "classKey",
                "orderKey",
                "familyKey",
                "genusKey",
                "speciesKey",
                "taxonKey",
                "year",
                "occurrenceStatus",
                "hasGeospatialIssues")
            .filter(
                "decimalLatitude IS NOT NULL AND "
                    + "decimalLongitude IS NOT NULL AND "
                    + "hasGeospatialIssues = false AND "
                    + "occurrenceStatus='PRESENT' ")
            .repartition(spark.sparkContext().conf().getInt("spark.sql.shuffle.partitions", 1200));

    spark.sql("DROP TABLE IF EXISTS point_map_input");
    source.write().format("parquet").saveAsTable("point_map_input");
    source = spark.read().table("point_map_input"); // defensive: avoid lazy evaluation

    MapKeysUDF mapKeysUDF = new MapKeysUDF(new HashSet<>(), true);
    Dataset<Row> stats =
        source
            .flatMap(row -> Arrays.asList(mapKeysUDF.call(row)).iterator(), Encoders.STRING())
            .toDF("mapKey")
            .groupBy("mapKey")
            .count()
            .withColumnRenamed("count", "occCount")
            .filter("occCount>=250000");

    List<Row> statsList = stats.collectAsList();
    return statsList.stream().map(s -> (String) s.getAs("mapKey")).collect(Collectors.toSet());
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
}
