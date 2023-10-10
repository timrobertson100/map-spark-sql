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

import org.gbif.demo.SaltPrefixPartitioner;
import org.gbif.demo.udf.EncodeBorYearUDF;
import org.gbif.demo.udf.HBaseKeyUDF;
import org.gbif.demo.udf.MapKeysUDF;
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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import lombok.AllArgsConstructor;
import scala.Tuple2;

import static org.gbif.maps.io.PointFeature.PointFeatures;
import static org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord;

@AllArgsConstructor
public class TilePointsSnapshot2 {
  private final String source;
  private final String hiveDB;
  private final String zkQuorum;
  private final String targetTable;
  private final int modulo;
  private final String targetDir;
  private final int threshold;

  public static void main(String[] args) throws IOException {
    TilePointsSnapshot2 driver =
        new TilePointsSnapshot2(
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

    prepareInput(spark);

    HBaseKeyUDF.registerPointKey(spark, "hbaseKey", new ModulusSalt(modulo));
    Dataset<Row> t1 =
        spark.sql(
            "SELECT "
                + "    /*+ BROADCAST(map_stats) */ " // efficient threshold filtering
                + "    hbaseKey(m.mapKey), collect_list(struct(lat, lng, borYear, occCount)) AS features "
                + "  FROM "
                + "    point_map_input m "
                + "    LEFT JOIN point_map_stats s ON m.mapKey = s.mapKey " // threshold filter
                + "  WHERE s.mapKey IS NULL "
                + "  GROUP BY m.mapKey");
    t1.createOrReplaceTempView("t1");

    JavaPairRDD<String, byte[]> t2 =
        t1.javaRDD()
            .mapToPair(
                (PairFunction<Row, String, byte[]>)
                    row -> {
                      String saltedKey = row.getString(0);
                      List<Row> tileData = row.getList(1);

                      PointFeatures.Builder tile = PointFeatures.newBuilder();
                      PointFeatures.Feature.Builder feature = PointFeatures.Feature.newBuilder();

                      tileData.stream()
                          .forEach(
                              f -> {
                                String bor = EncodeBorYearUDF.bor(f.getAs("borYear"));
                                Integer year = EncodeBorYearUDF.year(f.getAs("borYear"));
                                year = year == null ? 0 : year;

                                feature.setLatitude(f.getAs("lat"));
                                feature.setLongitude(f.getAs("lng"));
                                feature.setBasisOfRecord(BasisOfRecord.valueOf(bor));
                                feature.setYear(year);

                                tile.addFeatures(feature.build());
                                feature.clear();
                              });
                      byte[] mvt = tile.build().toByteArray();
                      return new Tuple2<>(saltedKey, mvt);
                    });

    ModulusSalt salter = new ModulusSalt(modulo);
    t2.repartitionAndSortWithinPartitions(new SaltPrefixPartitioner(salter.saltCharCount()))
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
  private void prepareInput(SparkSession spark) {
    Dataset<Row> source =
        spark
            .read()
            .format("com.databricks.spark.avro")
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
                    + "occurrenceStatus='PRESENT' ");

    // Source represents many small files so we repartition to remove NN pressure and task churn.
    // 1200 provides 50MB compressed parquet files from 2.5B input
    Dataset<Row> partitioned =
        source.repartition(
            spark.sparkContext().conf().getInt("spark.sql.shuffle.partitions", 1200)); //
    spark.sql("DROP TABLE IF EXISTS occurrence_input");
    partitioned.write().format("parquet").saveAsTable("occurrence_input");

    /*
    Set<String> omit = new HashSet<>();
    MapKeysUDF.appendNonNull(omit, "ALL", 0);
    for (int i = 0; i <= 8; i++) MapKeysUDF.appendNonNull(omit, "TAXON", i);
    MapKeysUDF.appendNonNull(omit, "TAXON", 56);
    MapKeysUDF.appendNonNull(omit, "TAXON", 212);
    MapKeysUDF.appendNonNull(omit, "TAXON", 216);
    MapKeysUDF.appendNonNull(omit, "TAXON", 729);
    MapKeysUDF.appendNonNull(omit, "DATASET", "4fa7b334-ce0d-4e88-aaae-2e0c138d049e"); // EOD
    MapKeysUDF.appendNonNull(omit, "DATASET", "38b4c89f-584c-41bb-bd8f-cd1def33e92f"); // artpo
    MapKeysUDF.appendNonNull(omit, "DATASET", "8a863029-f435-446a-821e-275f4f641165"); // obs.org
    MapKeysUDF.appendNonNull(omit, "DATASET", "50c9509d-22c7-4a22-a47d-8c48425ef4a7"); // iNat
     */

    MapKeysUDF.register(spark, "mapKeys");
    EncodeBorYearUDF.register(spark, "encodeBorYear");
    spark.sql("DROP TABLE IF EXISTS point_map_input");
    spark.sql(
        "CREATE TABLE point_map_input STORED AS parquet AS "
            + "SELECT "
            + "  mapKey, "
            + "  decimalLatitude AS lat, "
            + "  decimalLongitude AS lng, "
            // + "  encodeBorYear(basisOfRecord, year) AS borYear, " // improves performance
            + "  basisOfRecord, "
            + "  year, "
            + "  count(*) AS occCount "
            + "FROM "
            + "  occurrence_input "
            + "  LATERAL VIEW explode(  "
            // + "    array_distinct("
            + "      array("
            + "        concat_ws(':', '1', CAST(kingdomKey AS String)), "
            + "        concat_ws(':', '1', CAST(phylumKey AS String)), "
            + "        concat_ws(':', '1', CAST(classKey AS String)), "
            + "        concat_ws(':', '1', CAST(orderKey AS String)), "
            + "        concat_ws(':', '1', CAST(familyKey AS String)), "
            + "        concat_ws(':', '1', CAST(genusKey AS String)), "
            + "        concat_ws(':', '1', CAST(speciesKey AS String)), "
            + "        concat_ws(':', '1', CAST(taxonKey AS String)),"
            + "        concat_ws(':', '2', datasetKey), "
            + "        concat_ws(':', '3', publishingOrgKey), "
            + "        concat_ws(':', '4', countryCode), "
            + "        concat_ws(':', '5', publishingCountry)"
            + "      )"
            // TODO network
            // + "    ) "
            + "  ) m AS mapKey "
            + "GROUP BY mapKey, lat, lng, basisOfRecord, year");

    // Broadcasting a stats table proves faster than a windowing function and is simpler to grok
    spark.sparkContext().setJobDescription("Creating input stats using threshold of " + threshold);
    spark.sql("DROP TABLE IF EXISTS point_map_stats");
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS point_map_stats STORED AS PARQUET AS "
                + "SELECT mapKey, count(*) AS total "
                + "FROM point_map_input "
                + "GROUP BY mapKey "
                + "HAVING count(*) >= %d",
            threshold));
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
