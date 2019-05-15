package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd._
import org.apache.spark.sql.Row

object PreprocessingDataFrame_csvjson {
  def run(spark:SparkSession, df:DataFrame) = {
    val columns:  Array[String] = df.columns
    val parsed_df = df.distinct().rdd
   // parsed_df.foreach(println)
    val extractedentity_NoPredicatesRDD:RDD[(String,String)] = extractedentity_NoPredicates(parsed_df)
    //extractedentity_NoPredicatesRDD.foreach(println)
    val extractedentity_AllPredicatesRDD:RDD[(String,String)] = extractedentity_AllPredicates(parsed_df, columns)
    //extractedentity_AllPredicatesRDD.foreach(println)
    val extractedentity_SORDD:RDD[(String,String)] = extractedentity_SO(parsed_df)
    //extractedentity_SORDD.foreach(println)
    val extractedentiy_SPORDD: RDD[(String,String)] = extractedentity_SPO(parsed_df, columns)
    //extractedentiy_SPORDD.foreach(println)
    LSHoperation.run_dirty(spark, extractedentity_NoPredicatesRDD)
  }
  def run_clean(spark:SparkSession, df1: DataFrame, df2: DataFrame) = {
    val columns1: Array[String] = df1.columns
    val columns2: Array[String] = df2.columns
    
    val parsed_df1 = df1.distinct().rdd
    val parsed_df2 = df2.distinct().rdd
    val vocab_size_entity1 = columns1.length + parsed_df1.distinct().count()
    val vocab_size_entity2 = columns2.length + parsed_df2.distinct().count()
    val extractedentity1 = extractedentity_NoPredicates(parsed_df1)
    val extractedentity2 = extractedentity_NoPredicates(parsed_df2)
    LSHoperation.run(spark, extractedentity1, extractedentity2, vocab_size_entity1.toInt, vocab_size_entity2.toInt)
  }
  
  def extractedentity_NoPredicates(df:RDD[Row]) :RDD[(String,String)]= {
   return df.map(f=>{
     val key = f.get(0)+""
     var value :String = ""
     var i = 1
     while(i < f.length)
     {
       value += f.get(i) + " " 
       i += 1
     }
     value = value.replace(",", " ").stripSuffix(" ")
     value = value.replaceAll(" null", "").replaceAll("/", "").replaceAll(" - ", " ")
     (key,value)
   })
  }
  
  def extractedentity_AllPredicates(df:RDD[Row],columns:Array[String]) = {
    df.map(f=>{
      val key = f.get(0)+""
      var value = ""
      var i = 1
      while(i < f.length){
        value += columns(i)+ " " + f.get(i) + " "
        i += 1
      }
      (key,value.replace(",", " ").stripSuffix(" "))
    })
  }
  
  def extractedentity_SO(df:RDD[Row]):RDD[(String,String)] = {
    return extractedentity_NoPredicates(df).map(f=>{
     (f._1,f._1.split("/").last + " " + f._2)
   })
  }
  
  def extractedentity_SPO(df:RDD[Row],columns:Array[String]):RDD[(String,String)] = {
    return extractedentity_AllPredicates(df,columns).map(f=>{
      (f._1,f._1.split("/").last + " " + f._2)
    })
  }
  
}