import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._


object WordCount {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Donald Trump Word Count").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val tweets          = sc.cassandraTable("data","data")
    val tweets_filtered = tweets.filter(r => r.getString("content").length() > 2 && !(r.getString("content").charAt(0) == '"' && r.getString("content").charAt(1) == '@'))
    val words = tweets_filtered.flatMap(r => r.getString("content").split(" ")).filter(x=>x.length > 0)
                         .map( word => (word.toLowerCase().split("").filter(x=>x.charAt(0).isLetter).fold("")( _+_), 1))
                         .reduceByKey(_ + _)
                         .sortBy(_._2, false)
    val tweet_c = tweets_filtered.count
    val words_c = words.count
    val table = words.map(r => {
                               val (w, c) = r
                               ("realdonaldtrump", c, w, tweet_c, words_c)
                         })
    table.saveToCassandra("data","words")
  }
}
