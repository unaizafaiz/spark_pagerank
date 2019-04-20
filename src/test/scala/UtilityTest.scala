import java.util

import org.apache.hadoop.io.Text
import org.junit.Test

class UtilityTest {

  @Test def XMLParseTestEmptyList(): Unit ={

    val xml = <dblp>
      <inproceedings mdate="2017-05-28\" key="journals/acta/Saxena96">
        <author>Sanjeev Saxena</author>
        <title>Parallel Integer Sorting and Simulation Amongst CRCW Models.</title>
        <pages>607-619</pages>
        <year>1996</year>
        <volume>33</volume>
        <booktitle>Acta Inf.</booktitle>
        <number>7</number>
        <url>db/journals/acta/acta33.html#Saxena96</url>
        <ee>https://doi.org/10.1007/BF03036466</ee>
      </inproceedings></dblp>
    val actualOutput = Utility.parse(new Text(xml.toString()))
    val list = List
    val expectedOutput = list
    println("Actual Output:\n"+actualOutput)
    assert(actualOutput.length==0)
  }

  @Test def XMLParseTest_checkInproceedingsForNonEmptyList(): Unit ={

    val xml = <dblp>
      <inproceedings mdate="2017-05-28\" key="journals/acta/Saxena96">
        <author>Mark Grechanik</author>
        <title>Parallel Integer Sorting and Simulation Amongst CRCW Models.</title>
        <pages>607-619</pages>
        <year>1996</year>
        <volume>33</volume>
        <booktitle>Acta Inf.</booktitle>
        <number>7</number>
        <url>db/journals/acta/acta33.html#Saxena96</url>
        <ee>https://doi.org/10.1007/BF03036466</ee>
      </inproceedings></dblp>
    val actualOutput = Utility.parse(new Text(xml.toString()))
    val list = List
    val expectedOutput = list
    println("Actual Output:\n"+actualOutput)
    assert(actualOutput.length==1)
  }


  @Test def replaceUnicodeTest(): Unit ={

    val xml = <dblp>
      <inproceedings mdate="2017-05-28\" key="journals/acta/Saxena96">
        <author>Pavel Pudl&aacute;k</author>
        <author>Vojtech R&ouml;dl</author>
        <author>Petr Savick&yacute;</author>
      </inproceedings></dblp>
    val actualOutput = Utility.replaceUnicode(new Text(xml.toString()))
    val expectedOutput =  <dblp>
      <inproceedings mdate="2017-05-28\" key="journals/acta/Saxena96">
        <author>Pavel Pudlak</author>
        <author>Vojtech Rodl</author>
        <author>Petr Savicky</author>
      </inproceedings></dblp>
    println("Actual Output:\n"+actualOutput)
    assert(expectedOutput.toString().equals(actualOutput))
  }

  @Test def createPairsTest(): Unit ={
    val a1 = new Text("a1")
    val a2 = new Text("a2")
    val a3 = new Text("a3")
    //val list = Seq(a1,a2,a3)
    val list = new util.ArrayList[Text]
    list.add(a1)
    list.add(a2)
    list.add(a3)
    val pairs = Utility.createPairs(list)
    println("ActualOutput:\n"+pairs)
    assert(pairs.size==3)
  }

  @Test def createPairsOneElementTest(): Unit ={
    val a1 = new Text("a1")
   // val list = Seq(a1)
    val list = new util.ArrayList[Text]
    list.add(a1)
    val pairs = Utility.createPairs(list)
    println("ActualOutput:\n"+pairs)
    assert(pairs.size==0)
  }

  @Test def replaceAliasTest():Unit = {
    val authors = List(new Text("Georgeta Elisabeta Marai"), new Text("Debaleena Chattopadhyay"))
    val actualOutput = Utility.replaceAuthorAlias(authors)
    val expectedOutput = new util.ArrayList[Text]
    expectedOutput.add(new Text("G. Elisabeta Marai"))
    expectedOutput.add(new Text("Debaleena Chattopadhyay"))
    println("Actual Output:\n"+actualOutput)
    assert(actualOutput.equals(expectedOutput))
  }

}
