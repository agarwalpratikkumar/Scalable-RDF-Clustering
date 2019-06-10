package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import shapeless.LowPriority.For

object AnHaiGroup_Datasets {  
  
  // imdb dataset: https://datasets.imdbws.com/         (details of classes: https://www.imdb.com/interfaces/)
  
  //https://sites.google.com/site/anhaidgroup/useful-stuff/data#TOC-Miscellaneous-Data-Sets
  
  def run(spark: SparkSession) {  
    
    /*data = data.na.fill("null")
    
    //change id values of the file - make one file then remove this later - start 
    val new_anim_planet = data.rdd.map(f => {
      val id = f.get(0).toString()
      val new_id = "itune_ebook_"+id
      (new_id+"\t\t"+f.get(1).toString()+"\t\t"+f.get(2).toString()+"\t\t"+f.get(3).toString()+"\t\t"+f.get(4).toString()+"\t\t"+f.get(5).toString()+"\t\t"+f.get(6).toString()+"\t\t"+f.get(7).toString())
    })
    //new_anim_planet.take(20).foreach(println)
    
    new_anim_planet.coalesce(1).saveAsTextFile("/home/pratik/Downloads/itune_ebook_1_changed_id")
    //change id values of the file - make one file then remove this later - end
    */ 
    
    var all_predicate: RDD[(String,String)] = spark.sparkContext.emptyRDD[(String,String)]
    
    //,"music_itunes.csv","rotten_tomatoes.csv","movie_rating_3_imdb_rotten_tomatoes.csv","ebbok_2_itunes.csv","rate_beer.csv","beer_advocate.csv","book_3_half.csv","book_3_barnes_and_noble.csv","anime.csv","baby_products.csv"
    val files:List[String] = List("restaurant.csv","bikes.csv")
    for(file_names <- files){ 
      all_predicate = all_predicate.union(load_csv_content(spark, file_names))
    }
    
        
    LSHoperation.run_dirty(spark, all_predicate)
    
  }
  
  def load_csv_content(spark: SparkSession, file_name: String) : RDD[(String, String)] = {
    var data = spark.sqlContext.read.format("csv").option("header", "true").load("/home/pratik/Downloads/Thesis_Dataset/"+file_name)
    var columns = data.columns
    var data_rdd = data.rdd    
    return extract_predicates(data_rdd, columns)
  }
  
    def extract_predicates(entity: RDD[Row], columns: Array[String]) : RDD[(String, String)] = {
      return entity.map(f => {
        val key = f.get(0)+""
        var value = ""
        var i = 1
        while (i < f.length) {
          value += columns(i) +" "
          i += 1
      }   
      (key, value.stripSuffix(" "))
    })    
  }
}