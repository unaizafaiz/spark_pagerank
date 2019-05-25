import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Apache Spark implementation of PageRank
  * To find the pagerank values for nodes in dblp dataset
  * where each node is either an author or venue where an article is published
  */

object PageRank {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    logger.debug("Starting execution of Apache Spark for DBLP PageRank")

    if (args.length < 1) {
      logger.error("Usage: PageRank <inputfile> <output_directiory_path> <iter>")
      System.exit(1)
    }

    //getting no. of iterations or setting it to 10 if not specified
    val iters = if (args.length > 1) args(2).toInt else 10

    logger.debug("Creating SparkContext")
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setAppName("DBLPPageRank")
    val sc = new SparkContext(conf)

    logger.debug("Configuring XMLInputFormat for Spark")
    // This will detect the tags including attributes
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<inproceedings")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</inproceedings>")

    //getting each RDD based on the XML tag set above
    val records = sc.newAPIHadoopFile(
      args(0),
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])

    //Parse xml and get list of pairs of links for each <inproceedings> tag
    //filter if a record returns an empty list
    val link = records.map{s=>
      val list = Utility.parse(s._2)
      list
    }.filter(x=>x.nonEmpty)

    //flatten List[List] to List[]
    val edges = link.flatMap(_.map(o=>o))
    logger.debug("edges: "+edges)

    //split each value to get the two node between which a link exists
    val nodes = edges.map{ s =>
        val parts = s.toString.split("\t")
        (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    //set value of all links for a key to 1.0
    //must be var because the value changes during each iteration
    var ranks = nodes.mapValues(v => 1.0)

    //iterate specified times to find page rank values
    for (i <- 1 to iters) {
      logger.debug("Iteration: "+i)

      //calculating contributions of each node based on rank and no. of outlinks from that node
      val pageRankVal = nodes.join(ranks).values.flatMap{
        case (nodes, rank) =>
        val size = nodes.size
        nodes.map(node => (node, rank / size))
      }

      //recalculate ranks based on the formula
      ranks = pageRankVal
        .reduceByKey(_ + _)
        .mapValues(0.15 + 0.85 * _)
    }

    //saving the pageranks to an output directory
    ranks.saveAsTextFile(args(1))

    sc.stop()
  }
}
