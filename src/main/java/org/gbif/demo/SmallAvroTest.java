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

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import lombok.AllArgsConstructor;
import scala.Tuple2;

@AllArgsConstructor
public class SmallAvroTest {

  // https://stackoverflow.com/questions/38910968/reading-writing-avro-file-in-spark-core-using-java
  public static <T> JavaPairRDD<String, T> loadAvroFile(JavaSparkContext sc, String avroPath) {
    JavaPairRDD<AvroKey, NullWritable> records =
        sc.newAPIHadoopFile(
            avroPath,
            AvroKeyInputFormat.class,
            AvroKey.class,
            NullWritable.class,
            sc.hadoopConfiguration());
    return records
        .keys()
        .map(x -> (GenericRecord) x.datum())
        .mapToPair(pair -> new Tuple2<>((String) pair.get("key"), (T) pair.get("value")));
  }

  public static JavaRDD<GenericRecord> loadAvroFile2(JavaSparkContext sc, String avroPath) {
    JavaPairRDD<AvroKey, NullWritable> records =
        sc.newAPIHadoopFile(
            avroPath,
            AvroKeyInputFormat.class,
            AvroKey.class,
            NullWritable.class,
            sc.hadoopConfiguration());
    return records.keys().map(x -> (GenericRecord) x.datum());
  }

  public static JavaRDD<GenericRecord> loadAvroFile3(JavaSparkContext sc, String avroPath) {
    JavaPairRDD<AvroKey, NullWritable> records =
        sc.newAPIHadoopFile(
            avroPath,
            CombineAvroKeyFileInputFormat.class,
            AvroKey.class,
            NullWritable.class,
            sc.hadoopConfiguration());
    return records.keys().map(x -> (GenericRecord) x.datum());
  }

  public static JavaRDD<GenericRecord> loadAvroFile4(JavaSparkContext sc, String avroPath) {
    JavaPairRDD<AvroKey, NullWritable> records =
        sc.newAPIHadoopFile(
            avroPath,
            AvroKeyCombineFileInputFormat.class,
            AvroKey.class,
            NullWritable.class,
            sc.hadoopConfiguration());
    return records.keys().map(x -> (GenericRecord) x.datum());
  }

  public static void main(String[] args) throws IOException {

    SparkSession spark =
        SparkSession.builder().appName("Small avro test").enableHiveSupport().getOrCreate();

    String avroPath = "/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence";

    // 72284 15 mins
    // JavaRDD<GenericRecord> data =
    //    loadAvroFile3(JavaSparkContext.fromSparkContext(spark.sparkContext()), avroPath);

    // 1 task
    // JavaRDD<GenericRecord> data =
    //    loadAvroFile3(JavaSparkContext.fromSparkContext(spark.sparkContext()), avroPath);

    // 2306 tasks 9 mins
    JavaRDD<GenericRecord> data =
        loadAvroFile4(JavaSparkContext.fromSparkContext(spark.sparkContext()), avroPath);

    JavaRDD<String> d = data.map(x -> ((String) x.get("datasetkey")));
    d.saveAsTextFile("/tmp/datasets2.txt");

    // System.out.println(data.count());

    /*
    Dataset<Row> source =
        spark
            .read()
            .format("com.databricks.spark.avro")
            .load("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence")
            .select(
                "datasetkey",
                "publishingorgkey",
                "publishingcountry",
                "networkkey",
                "countrycode",
                "basisofrecord",
                "decimallatitude",
                "decimallongitude",
                "kingdomkey",
                "phylumkey",
                "classkey",
                "orderkey",
                "familykey",
                "genuskey",
                "specieskey",
                "taxonkey",
                "year",
                "occurrencestatus",
                "hasgeospatialissues");

    System.out.println(source.count());
    */
  }
}
