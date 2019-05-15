package net.sansa_stack.template.spark.rdf

import java.net.URI

import scala.collection.mutable

import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.rdd.RDD
import scala.reflect.internal.util.TableDef.Column
import net.sansa_stack.template.spark.rdf.test.test.TripleGetter

object TripleReader {

  def main(args: Array[String]) {
    //println("hello world!")
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Triple reader example  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")
    

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)
    //val triplesRDD = NTripleReader.load(spark, URI.create(input))
    //PreprocessingEntities.run(spark, triplesRDD)
    
    PreprocessingEntities.run(spark, triples)
    //TripleGetter.run()
    //testing_nearest_neighbour.run(spark)
    

    //triples.take(5).foreach(println(_))
    
    //Birt_Data.run(spark)
    

    /*var Employee_data = spark.read.csv(input)    
    val column_name =  Seq("employeeNumber", "lastName", "firstName", "extension", "email" , "officeCode" , "reportsTo", "jobTitle")
    Employee_data = Employee_data.toDF(column_name: _*)
    //Employee_data.show(false)
    
    val columns = Employee_data.columns    
    val employee_rdd = Employee_data.rdd
    //val all_pred_for_employees = PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(employee_rdd, columns)
    
    //all_pred_for_employees.foreach(println)
    val all_pred_for_employees = employee_rdd.map(f => {
      val key = f.get(0)+""
      var value = ""
      var i = 1
      while (i < f.length) {
        value += columns(i) +" "
        i += 1        
      }   
      (key, value.stripSuffix(" "))
    })
    
    
    var Customer_data = spark.read.csv("src/main/resources/Customers.csv")
    val customer_column_name =  Seq("customerNumber", "customerName", "contactLastName", "contactFirstName", "phone" , "addressLine1" , "addressLine2", "city", "state", "postalCode" ,"country" ,"salesRepEmployeeNumber", "creditLimit")
    Customer_data = Customer_data.toDF(customer_column_name: _*)
    //Customer_data.show(false)
    
    val customer_columns = Customer_data.columns    
    val customer_rdd = Customer_data.rdd
    //val all_pred_for_customer = PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(customer_rdd, customer_columns)
    
    val all_pred_for_customer = customer_rdd.map(f => {
      val key = f.get(0)+""
      var value = ""
      var i = 1
      while (i < f.length) {
        value += customer_columns(i) +" "
        i += 1        
      }   
      (key, value.stripSuffix(" "))
    })
    
    
    val all_predicate = all_pred_for_customer.union(all_pred_for_employees)
    //all_predicate.foreach(println)
    
    LSHoperation.run_dirty(spark, all_predicate)*/



    
    
    
    
    //triplesRDD.saveAsTextFile(output)
    
    //val rdd_binar = spark.sparkContext.binaryRecords("/home/pratik/Downloads/dataset",100)
    //val rdd_binar = spark.sparkContext.hadoopRDD(Broadcast<org.apache.spark.util.SerializableConfiguration>,binary, binary, 10)
    //val rdd_binar = spark.sparkContext.newAPIHadoopFile("/home/pratik/Downloads/groundtruth", classOf[TextInputFormat], classOf[Text],classOf[Text])
    //rdd_binar.take(5).foreach(println)
    //serial_test.run(spark, rdd_binar)
    
    
    //var hRDD = new NewHadoopRDD(spark.sparkContext, classOf[RandomAccessInputFormat], 
      //  classOf[IntWritable], 
       // classOf[BytesWritable]
        //)

//val count = hRDD.mapPartitionsWithInputSplit{ (split, iter) => myfuncPart(split, iter)}.collect()
    

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple reader example") {

    head(" Triple reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }
}