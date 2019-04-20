import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Apache Spark implementation of PageRank
  * To find the pagerank values for nodes in dblp dataset
  * where each node is either an author or venue where an article is published
  */

object PageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val iters = if (args.length > 1) args(2).toInt else 10

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
   // conf.setMaster("yarn")
    conf.setAppName("DBLPPageRank")
    val sc = new SparkContext(conf)

    // This will detect the tags including attributes
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<inproceedings")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</inproceedings>")

    //getting each RDD based on the XML tag set above
    val records = sc.newAPIHadoopFile(
      //args(0)
      "/Users/unaizafaiz/Downloads/dblp.xml",
     // "hdfs:///user/maria_dev/input/dblp.xml",
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])

    //Parse xml and get list of pairs of links for each <inproceedings> tag
    val link = records.map{s=>
      val list = Utility.parse(s._2)
      list
    }.filter(x=>x.nonEmpty)

    //flatten List[List] to List[]
    val urls_links = link.flatMap(_.map(o=>o))

    //split each value to get the two node between which a link exists
    val links = urls_links.map{ s =>
        val parts = s.toString.split("\t")
        (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    //set value of all links for a key to 1.0
    //must be var because the value changes during each iteration
    var ranks = links.mapValues(v => 1.0)

    //iterate specified times to find page rank values
    for (i <- 1 to iters) {

      //calculating contributions of each node based on rank and no. of outlinks from that node
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      //recalculate ranks based on the formula
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    //ranks.saveAsTextFile("hdfs:///tmp/output")
    ranks.saveAsTextFile("./src/main/resources/output")
   // ranks.saveAsTextFile(args(1))
    sc.stop()
  }
}
