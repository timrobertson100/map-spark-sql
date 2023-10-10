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

import org.gbif.demo.udf.MapKeysUDF;
import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;

@Builder
public class MapBuilder implements Serializable {
  private final String sourceDir;
  private final String hiveDB;
  private final String zkQuorum;
  private final String hivePrefix;
  private final String hbaseTable;
  private final int modulo;
  private final int tileSize;
  private final int bufferSize;
  private final int maxZoom;
  private final String targetDir;
  private final int threshold;
  private boolean buildPoints;
  private boolean buildTiles;

  public static void main(String[] args) throws IOException {
    // TODO: configuration
    MapBuilder points =
        MapBuilder.builder()
            .sourceDir("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/*.avro")
            .hiveDB("tim")
            .hivePrefix("points")
            .zkQuorum("c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181")
            .hbaseTable("tim")
            .modulo(100)
            .targetDir("/tmp/tim-points/")
            .threshold(25000)
            .buildPoints(true)
            .build();
    // points.run();

    MapBuilder tiles =
        MapBuilder.builder()
            // .sourceDir(
            //
            // "/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/50c9509d*.avro")
            .sourceDir("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/*.avro")
            .hiveDB("tim")
            .hivePrefix("tiles")
            .zkQuorum("c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181")
            .hbaseTable("tim")
            .modulo(100)
            .tileSize(512)
            .bufferSize(64)
            .maxZoom(16)
            .targetDir("/tmp/tim-tiles/")
            .threshold(25000)
            .buildTiles(true)
            .build();
    tiles.run();
  }

  void run() throws IOException {
    SparkSession spark =
        SparkSession.builder().appName("Map Builder").enableHiveSupport().getOrCreate();
    spark.sql("use " + hiveDB);
    spark.sparkContext().conf().set("hive.exec.compress.output", "true");

    // Read the source Avro files and prepare them as performant tables
    String inputTable = String.format("%s_map_input", hivePrefix);
    Dataset<Row> source = readAvroSource(spark, inputTable);

    // Determine the mapKeys of maps that require a tile pyramid
    Set<String> largeMapKeys = mapKeyExceedingThreshold(source);

    if (buildPoints) {
      PointMapBuilder.builder()
          .spark(spark)
          .sourceTable(inputTable)
          .largeMapKeys(largeMapKeys)
          .salter(new ModulusSalt(modulo))
          .targetDir(targetDir)
          .hadoopConf(hadoopConf())
          .build()
          .generate();
    }

    if (buildTiles) {
      TileMapBuilder.builder()
          .spark(spark)
          .sourceTable(inputTable)
          .largeMapKeys(largeMapKeys)
          .salter(new ModulusSalt(modulo))
          .tileSize(tileSize)
          .bufferSize(bufferSize)
          .maxZoom(maxZoom)
          .targetDir(targetDir)
          .hadoopConf(hadoopConf())
          .build()
          .generate();
    }
  }

  /**
   * Reads the input avro files, filtering for records of interest. The dataset is registered as a
   * Hive table to defend against lazy evaluation that may cause the input avro files to be read
   * multiple times.
   */
  private Dataset<Row> readAvroSource(SparkSession spark, String targetHiveTable) {
    Dataset<Row> source =
        spark
            .read()
            .format("com.databricks.spark.avro")
            .load(sourceDir)
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

    spark.sql(String.format("DROP TABLE IF EXISTS %s", targetHiveTable));
    source.write().format("parquet").saveAsTable(targetHiveTable);

    // we do not return source, as any lazy evaluations will re-read avro
    return spark.read().table(targetHiveTable);
  }

  /**
   * Extracts only those map keys that exceed the threshold of occurrence count, collected to the
   * Spark Driver
   */
  private Set<String> mapKeyExceedingThreshold(Dataset<Row> source) {
    MapKeysUDF mapKeysUDF = new MapKeysUDF(new HashSet<>(), true);
    Dataset<Row> stats =
        source
            .flatMap(row -> Arrays.asList(mapKeysUDF.call(row)).iterator(), Encoders.STRING())
            .toDF("mapKey")
            .groupBy("mapKey")
            .count()
            .withColumnRenamed("count", "occCount")
            .filter(String.format("occCount>=%d", threshold));

    List<Row> statsList = stats.collectAsList();
    return statsList.stream().map(s -> (String) s.getAs("mapKey")).collect(Collectors.toSet());
  }

  /** Creates the Hadoop configuration suitable for writing HFiles */
  private Configuration hadoopConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    conf.set(FileOutputFormat.COMPRESS, "true");
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
    Job job = new Job(conf, "Map tile build"); // name not actually used
    HTable table = new HTable(conf, hbaseTable);
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    return job.getConfiguration(); // job created a copy of the conf
  }
}
