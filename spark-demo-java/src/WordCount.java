import breeze.optimize.linear.LinearProgram;
import jdk.internal.util.xml.impl.Pair;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class WordCount {
    private static final String regexp = "[a-zA-Z0-9]+(\\.[a-zA-Z0-9])+(:\\d+)?[-A-Za-z0-9+&@#/%?=~_!:,.;]+[-A-Za-z0-9+&@#/%=~_]$";
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("wordcount.txt"));

        //需替换为自己的本地地址
        JavaRDD<String> lines = spark.read().textFile("/data/e_ctwap.log").javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s).iterator());

        Pattern pattern = Pattern.compile(regexp);

        words = words.map(word -> {
            Matcher matcher = pattern.matcher(word.toString());
            if (matcher.find()) {
                if (!matcher.group().matches("^(\\d+\\.)+\\d+$")) {
                    return matcher.group();
                }
            }
            return "";
        }).filter(s -> !s.equals(""));

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<Integer, String > count2 = counts.mapToPair(s -> new Tuple2<Integer, String >(s._2(), s._1()));

        JavaPairRDD<Integer, String > count3 = count2.sortByKey(false);

        List<Tuple2<Integer, String>> list = count3.take(10);

        list.forEach(s -> {
            try {
                bufferedWriter.write(s._2 + ": " + s._1 + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(s._2 + ": " + s._1 + "\n");
        });

        bufferedWriter.close();
        spark.stop();
    }
}