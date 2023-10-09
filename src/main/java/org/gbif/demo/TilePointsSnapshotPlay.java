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

import org.gbif.demo.udf.EncodeBorYearUDF;
import org.gbif.demo.udf.HBaseKeyUDF;
import org.gbif.demo.udf.MapKeysUDF;
import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.IOException;
import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;

import lombok.AllArgsConstructor;
import scala.Tuple2;

import static org.gbif.maps.io.PointFeature.PointFeatures;
import static org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord;

@AllArgsConstructor
public class TilePointsSnapshotPlay {
  private final String source;
  private final String hiveDB;
  private final String zkQuorum;
  private final String targetTable;
  private final int modulo;
  private final String targetDir;
  private final int threshold;
  private final Set<String> omitKeys;

  public static void main(String[] args) throws IOException {
    // TODO Consider if we want to go down this route of optimisation
    Set<String> omit = new HashSet<>();
    MapKeysUDF.appendNonNull(omit, "ALL", 0);
    for (int i = 0; i <= 8; i++) MapKeysUDF.appendNonNull(omit, "TAXON", i);
    MapKeysUDF.appendNonNull(omit, "TAXON", 56);
    MapKeysUDF.appendNonNull(omit, "TAXON", 212);
    MapKeysUDF.appendNonNull(omit, "TAXON", 216);
    MapKeysUDF.appendNonNull(omit, "TAXON", 729);
    MapKeysUDF.appendNonNull(omit, "DATASET", "4fa7b334-ce0d-4e88-aaae-2e0c138d049e"); // EOD
    MapKeysUDF.appendNonNull(
        omit, "DATASET", "38b4c89f-584c-41bb-bd8f-cd1def33e92f"); // artportalen
    MapKeysUDF.appendNonNull(omit, "DATASET", "8a863029-f435-446a-821e-275f4f641165"); // obs.org
    MapKeysUDF.appendNonNull(omit, "DATASET", "50c9509d-22c7-4a22-a47d-8c48425ef4a7"); // iNat

    TilePointsSnapshotPlay driver =
        new TilePointsSnapshotPlay(
            "occurrence_input",
            "tim",
            "c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181",
            "tim",
            100,
            "/tmp/tim-points",
            250000,
            omit);
    driver.run();
  }

  public static JavaRDD<GenericRecord> loadAvroFile(JavaSparkContext sc, String avroPath) {
    JavaPairRDD<AvroKey, NullWritable> records =
        sc.newAPIHadoopFile(
            avroPath,
            AvroKeyCombineFileInputFormat.class,
            AvroKey.class,
            NullWritable.class,
            sc.hadoopConfiguration());
    return records.keys().map(x -> (GenericRecord) x.datum());
  }

  private void run() throws IOException {
    SparkSession spark =
        SparkSession.builder().appName("Map Points").enableHiveSupport().getOrCreate();
    SparkConf conf = spark.sparkContext().conf();
    conf.set("hive.exec.compress.output", "true");
    spark.sql("use " + hiveDB);

    Dataset<Row> source2 =
        spark
            .read()
            .format("com.databricks.spark.avro")
            .load("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence")
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
            .repartition(spark.sparkContext().conf().getInt("spark.sql.shuffle.partitions", 600));
    spark.sql("DROP TABLE IF EXISTS occurrence_input");
    source2.write().format("parquet").saveAsTable("occurrence_input");

    // source2.persist();
    // source2.createOrReplaceTempView("occurrence_input");

    // TODO Consider if we want to go down this route of optimisation
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

    JavaRDD<String> counts =
        source2
            .javaRDD()
            .flatMap(
                row -> {
                  return Arrays.stream(
                          new MapKeysUDF(omit, false)
                              .call(
                                  row.getAs("kingdomKey"),
                                  row.getAs("phylumKey"),
                                  row.getAs("classKey"),
                                  row.getAs("orderKey"),
                                  row.getAs("familyKey"),
                                  row.getAs("genusKey"),
                                  row.getAs("speciesKey"),
                                  row.getAs("taxonKey"),
                                  row.getAs("datasetKey"),
                                  row.getAs("publishingOrgKey"),
                                  row.getAs("countryCode"),
                                  row.getAs("publishingCountry"),
                                  row.getAs("networkKey")))
                      .iterator();
                })
            .mapToPair(s -> new Tuple2<>(s, 1l))
            .reduceByKey((r1, r2) -> r1 + r2)
            .filter(r -> r._2 > 250000)
            .map(r -> r._1);

    // share the lookup with all executors
    Set<String> keysToIgnore = new HashSet<>(counts.collect());
    keysToIgnore.stream().forEach(s -> System.out.println("Ignoring " + s));

    /*
    Broadcast<Set> keysToIgnoreBroadcast =  spark
        .sparkContext()
        .broadcast(keysToIgnore, scala.reflect.ClassManifestFactory.fromClass(Set.class));
     */
    prepareInput(spark, keysToIgnore);

    HBaseKeyUDF.registerPointKey(spark, "hbaseKey", new ModulusSalt(modulo));
    Dataset<Row> t1 =
        spark.sql(
            "SELECT "
                + "    hbaseKey(m.mapKey), collect_list(struct(lat, lng, borYear, occCount)) AS features "
                + "  FROM "
                + "    point_map_input m "
                + "  WHERE s.mapKey IS NULL"
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
  private void prepareInput(SparkSession spark, Set<String> keysToExclude) {
    MapKeysUDF.register(spark, "mapKeys", keysToExclude, false); // exclude
    EncodeBorYearUDF.register(spark, "encodeBorYear");

    spark.sql("DROP TABLE IF EXISTS point_map_input");
    spark.sql(
        String.format(
            "CREATE TABLE point_map_input STORED AS parquet AS "
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
                + "GROUP BY mapKey, lat, lng, borYear",
            source));

    // Broadcasting a stats table proves faster than a windowing function and is simpler to grok
    /*
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

     */
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

  private void delme() {
    /*

        Dataset<Row> sample =
            spark
                .read()
                .format("com.databricks.spark.avro")
                .load(
                    "/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/fff51e2c-0b63-4a54-afdc-5c4be905129c_5.avro");
        StructType schemaSample = sample.schema();
        System.out.println("Read a sample schema:");
        System.out.println(schemaSample);
        System.out.println("Now using a combine format...");

        JavaRDD<GenericRecord> data =
            loadAvroFile(
                JavaSparkContext.fromSparkContext(spark.sparkContext()),
                "/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence");

        StructType schema =
            new StructType()
                .add("datasetkey", DataTypes.StringType, true)
                .add("publishingorgkey", DataTypes.StringType, true)
                .add("publishingcountry", DataTypes.StringType, true)
                .add("networkkey", DataTypes.createArrayType(DataTypes.StringType, true), true)
                .add("countrycode", DataTypes.StringType, true)
                .add("basisofrecord", DataTypes.StringType, true)
                .add("decimallatitude", DataTypes.DoubleType, true)
                .add("decimallongitude", DataTypes.DoubleType, true)
                .add("kingdomkey", DataTypes.IntegerType, true)
                .add("phylumkey", DataTypes.IntegerType, true)
                .add("classkey", DataTypes.IntegerType, true)
                .add("orderkey", DataTypes.IntegerType, true)
                .add("familykey", DataTypes.IntegerType, true)
                .add("genuskey", DataTypes.IntegerType, true)
                .add("specieskey", DataTypes.IntegerType, true)
                .add("taxonkey", DataTypes.IntegerType, true)
                .add("year", DataTypes.IntegerType, true)
                .add("occurrencestatus", DataTypes.StringType, true)
                .add("hasgeospatialissues", DataTypes.BooleanType, true);

        JavaRDD<Row> d =
            data.map(
                x ->
                    RowFactory.create(
                        (String)x.get("datasetkey"),
                        String.valueOf(x.get("publishingorgkey")),
                        String.valueOf(x.get("publishingcountry")),
                        (String[]) null, // x.get("networkkey")
                        String.valueOf(x.get("countrycode")),
                        String.valueOf(x.get("basisofrecord")),
                        (Double) x.get("decimallatitude"),
                        (Double) x.get("decimallongitude"),
                        (Integer) x.get("kingdomkey"),
                        (Integer) x.get("phylumkey"),
                        (Integer) x.get("classkey"),
                        (Integer) x.get("orderkey"),
                        (Integer) x.get("familykey"),
                        (Integer) x.get("genuskey"),
                        (Integer) x.get("specieskey"),
                        (Integer) x.get("taxonkey"),
                        (Integer) x.get("year"),
                        String.valueOf(x.get("occurrencestatus")),
                        (Boolean) x.get("hasgeospatialissues")));

        Dataset<Row> source = spark.createDataFrame(d, schema);
    */
  }
}
