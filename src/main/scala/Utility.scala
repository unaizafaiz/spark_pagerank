import scala.xml.XML
import java.util

import org.apache.hadoop.io.Text
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Utility Object containing methods for parsing the XML
  */

 object Utility{

  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Parse XML and get all edges between two nodes produced in a publication
    * @param value: Mapper ValueIN
    * @return List[Links between each node]
    */
   def parse(value: Text): List[Text] = {

     logger.info("Parse the text: "+value)
     //getting professors that are from UIC
     val uicProflist = profLookup()
     val text = replaceUnicode(value)
     val elem = XML.loadString(text)

     //Parsing xml for <inproceedings> tag
      var articles = (elem \\ "inproceedings")
      val listAuthors = new util.ArrayList[Text]

     //For each inproceeding build the edges it produces
     //author1 <--> author2
     //authorM ---> venue
      articles.foreach { n =>
        val pairs = new util.ArrayList[Text]()
        val authors = (n \ "author").map(i => new Text(i.text.toString))
        val filterauthors =  authors.filter(d => uicProflist.contains(d.toString)).map(i=>i)
        val updateAlias = replaceAuthorAlias(filterauthors)
        //updateAlias.add(new Text(venue.text.toString))
        if(!filterauthors.isEmpty) {
          val venue = (n \ "booktitle")
          val venueText = venue.text.toString
          val authorPair = Utility.createPairs(updateAlias)
          authorPair.map(i => {
            //Comparing author names to keep the pair in alphabetical order
            val swap = i._1.compareTo(i._2) > 0
            val author1 = if(swap) i._2 else i._1
            val author2 = if(swap) i._1 else i._2

            //Creating bidirectional links for each other pairs
            listAuthors.add(new Text(author1.toString+"\t"+author2.toString))
            listAuthors.add(new Text(author2.toString+"\t"+author1.toString))
          })

          //creating outlink for each author and the venue
          val iter = updateAlias.iterator()
          while(iter.hasNext){
            listAuthors.add(new Text(iter.next()+"\t"+venueText))
          }
        }
      }

     //Converting java.util.Arraylist to scala List[]
     val array = listAuthors.toArray()
     val list = array.toList.map(_.asInstanceOf[Text])

     logger.info("Found list: "+list)

      return list
    }

  /**
    * Create a lookup list of all professors
    * @return
    */
  def profLookup(): util.ArrayList[String] = {

    logger.info("In profLookup()")
    val uicProflist = new util.ArrayList[String]()
    val fileStream = getClass.getResourceAsStream("/prof_name_list.txt")
    val lines = Source.fromInputStream(fileStream).getLines
    lines.foreach(line => {
      val cols = line.split(",").map(_.trim)
      cols.foreach(uicProflist.add(_))
    })

    return uicProflist
  }


  /**
    * Create a Hashmap of professors with more than one alias used in publication
    * @return
    */
  def profMap(): util.HashMap[String,String] = {
    logger.info("In profMap()")
    val uicProfMap = new util.HashMap[String,String]()
    val fileStream = getClass.getResourceAsStream("/prof_name_list.txt")
    val lines = Source.fromInputStream(fileStream).getLines
    lines.foreach(line => {
      val cols = line.split(",").map(_.trim)
      val alias = cols.toList
      if(alias.size>1)
        alias.foreach(name=>{uicProfMap.put(name, cols(0))})
    })
    return uicProfMap
  }

  /**
    *
    * @param authors Seq of authors
    * @return updated author list with alias replaced to avoid differentiating same author
    */
   def replaceAuthorAlias(authors: Seq[Text]): util.ArrayList[Text] ={
     logger.info("Replacing author alias with author name")
     val uicProfMap = profMap()
     val updatedAuthors = new util.ArrayList[Text]
     authors.foreach(author =>{
       if(uicProfMap.containsKey(author.toString)) {
         logger.info("replacing ... "+author.toString)
         updatedAuthors.add(new Text(uicProfMap.get(author.toString)))
       }else{
         updatedAuthors.add(author)
       }
     })
     return updatedAuthors
   }


  /**
    * Create  pair of elements from a list of objects
    * @param article: ArrayList
    * @return List of pairs for elements in list
    */
   def createPairs(article: util.ArrayList[Text]): List[(Text,Text)] = {
     logger.debug("creating pairs from a list: "+article)
     val pairs = article.asScala.toList.combinations(2)
       .map { case List(a, b) =>

         new Tuple2 (a, b) }.toList

     logger.debug("Pairs created: "+pairs)
     return pairs
   }

  /**
    * Replace the unicode characters in XML
     * @param value XML Text
    * @return String of cleaned XML
    */
   def replaceUnicode(value: Text): String ={
     logger.debug("Replacing unicode characters");
     val text =value.toString()
     val unicode = Map("&Agrave;"->   "A",
       "&Aacute;"->   "A",
       "&Acirc;"->    "A",
       "&Atilde;"->  "A",
       "&Auml;"->    "A",
       "&Aring;"->   "A",
       "&AElig;"->   "AE",
       "&Ccedil;"->   "C",
       "&Egrave" ->  "E",
       "&Eacute;"->   "E",
       "&Ecirc;"->    "E",
       "&Euml;"->    "E",
       "&Igrave;"->  "I",
       "&Iacute;"->   "I",
       "&Icirc;"->   "I",
       "&Iuml;"->     "I",
       "&ETH;"->     "Eth",
       "&Ntilde;"->   "N",
       "&Ograve;"->   "O",
       "&Oacute;"->   "O",
       "&Ocirc;"->    "O",
       "&Otilde;"->  "O",
       "&Ouml;"->     "O",
       "&Oslash;"->   "O",
       "&Ugrave;"->   "U",
       "&Uacute;"->   "U",
       "&Ucirc;"->   "U",
       "&Uuml;"->    "U",
       "&Yacute;"-> "Y",
       "&THORN;"->   "THORN",
       "&szlig;"->   "s",
       "&agrave;"->  "a",
       "&aacute;"->  "a",
       "&acirc;"->    "a",
       "&atilde;"->  "a",
       "&auml;"->   "a",
       "&aring;"->   "a",
       "&aelig;"->   "ae",
       "&ccedil;"-> "c",
       "&egrave;"->  "e",
       "&eacute;"->  "e",
       "&ecirc;"->    "e",
       "&euml;"->    "e",
       "&igrave;"->  "i",
       "&iacute;"->  "i",
       "&icirc;"->   "i",
       "&iuml;"->    "i",
       "&eth;"->      "eth",
       "&ntilde;"->   "n",
       "&ograve;"->  "o",
       "&oacute" ->  "o",
       "&ocirc;"->  "o",
       "&otilde;"->  "o",
       "&ouml;"->   "o",
       "&oslash;"->  "o",
       "&ugrave;"->  "u",
       "&uacute;"-> "u",
       "&ucirc;"->  "u",
       "&uuml;"->  "u",
       "&yacute;"->"y",
       "&thorn;"->"thorn",
       "&yuml;"->"y",
       "&times;" -> "x",
       "&reg;"->"(REG)",
        "&micro;"->"micro(symb)")
     return unicode.foldLeft(text) { case (cur, (from, to)) => cur.replaceAll(from, to) }
   }

}


