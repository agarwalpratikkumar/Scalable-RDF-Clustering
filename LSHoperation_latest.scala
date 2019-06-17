package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ Tokenizer, HashingTF }
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.util.regex._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg._
import scala.collection.mutable
import org.apache.spark.RangePartitioner
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.ml.feature._

//import org.graphframes._
//import org.graphframes.GraphFrame

object LSHoperation {
  
  var Arr_val:ArrayBuffer[Set[List[String]]] = _
  var j = -1
  
  def run_dirty(spark: SparkSession, extractedEntity: RDD[(String, String)]) = {
    //extractedEntity.foreach(println)

    val featuredData_Df: DataFrame = vectoriseText(spark, extractedEntity)

    //K_Means.run(spark, featuredData_Df)
    val (model: MinHashLSHModel, transformedData_Df: DataFrame) = minHashLSH(featuredData_Df)

    transformedData_Df.show(false)
    
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df)
    //println("approx sim join: ")
    //dataset.columns.foreach(println)    //entity1,entity2,distance
    //dataset.show(false)
    matchentities(spark,dataset, featuredData_Df)
    
  }  
  
  def remove_stopwords(tokenizedData_Df: DataFrame): DataFrame = {
    val remover =  new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val removed_df = remover.transform(tokenizedData_Df)
    //removed_df.withColumn("filtered_words", when(col("filtered_words").contains(", null"), "") otherwise(col("filtered_words")))
    //removed_df.show(false)
    return remover.transform(tokenizedData_Df)
  }
  def vectoriseText(spark: SparkSession, entities: RDD[(String, String)]): DataFrame = {
   
    val entityProfile_Df = spark.createDataFrame(entities).toDF("entities", "attributes")
    entityProfile_Df.show(false)
    //entityProfile_Df.select("attributes").distinct()
    val tokenizer = new Tokenizer().setInputCol("attributes").setOutputCol("words")
    val tokenizedData_Df = tokenizer.transform(entityProfile_Df)
    tokenizedData_Df.show(false)
    val cleanData_Df = remove_stopwords(tokenizedData_Df).distinct
    val cleandfrdd = cleanData_Df.select("filtered_words").distinct.rdd
    val vocab_size = calculate_Vocabsize(cleandfrdd)
    println("vocab_size:"+vocab_size)
    //cleanData_Df.select("entities", "filtered_words").show(false)
    //val vocabSize = 1000000
    //cleanData_Df.select("filtered_words").rdd.foreach(println)
  /*  val hashingTf = new HashingTF().setInputCol("filtered_words").setOutputCol("raw_Features").setNumFeatures(vocab_size)
    val featuredData_hashedDf = hashingTf.transform(cleanData_Df)
    //featuredData_hashedDf.show(false)
    val idf = new IDF().setInputCol("raw_Features").setOutputCol("features")
    val idfModel = idf.fit(featuredData_hashedDf)
    val rescaledHashedData = idfModel.transform(featuredData_hashedDf)
    //rescaledHashedData.show(false)*/
    val cvModel = new CountVectorizer().setInputCol("filtered_words").setOutputCol("features").setMinDF(1).setVocabSize(Math.round(0.70*vocab_size).toInt).fit(cleanData_Df)
    val featuredData_cvDf = cvModel.transform(cleanData_Df)
    
    //return rescaledHashedData
    return featuredData_cvDf
  }
  
  def calculate_Vocabsize(cleandfrdd:RDD[Row]): Int = {
    var s = cleandfrdd.map(_.mkString.replace("WrappedArray(", "").replace("(", "").replace(")", ", ")).reduce(_+_).split(", ").toSet  
    s-=("-","/")
    //s.foreach(println)
    return s.size
  }

  def minHashLSH(featuredData_Df: DataFrame): (MinHashLSHModel, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("HashedValues")
    val model = mh.fit(featuredData_Df)
    val transformedData_Df = model.transform(featuredData_Df)
    //println("min hash: ")
    //transformedData_Df.show(false)
    return (model, transformedData_Df)
  }

  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df: DataFrame): Dataset[_] = {
    val threshold = 0.40
    return model.approxSimilarityJoin(transformedData_Df, transformedData_Df, threshold)
  }
  
  def matchentities(spark:SparkSession,dataset:Dataset[_], featuredData_Df:DataFrame) = {
    println("entered matched entities: ")
    
    val refined_entities_dataset = dataset
    .filter("datasetA.entities != datasetB.entities")
    .select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2"))  // ,col("distCol")
    
    var entity_dataset = refined_entities_dataset.repartition(200).persist(StorageLevel.MEMORY_AND_DISK) 
    //entity_dataset.show(false) 
    
    
   /*val combined_ = entity_dataset.rdd.map(f => {
      if(f.getString(0).length() <= f.getString(1).length()) {
       // list_col = list_col :+ f.getString(0)
        (f.getString(0),f.getString(1))   
      }
      else 
      {
        //list_col = list_col :+ f.getString(1)
        (f.getString(1),f.getString(0))
      }
      
    }).distinct()*/
    
   import spark.implicits._
    //var df_toChange = combined_.toDF("entity1","entity2")          //these are working, just trying below from entity_dataset
    //val set_v = combined_.collect().toSet
    
    //var df_toChange = entity_dataset
    val set_v = entity_dataset.rdd.collect().toSet   
    
    set_v.map(f => {
      entity_dataset = entity_dataset.withColumn("entity1", regexp_replace(col("entity1"), f.getString(1), f.getString(0)))  
      })
    //println("reduced entities: ")
    //entity_dataset.show(false)
    
    val clusters = entity_dataset.rdd.map(f => {
      val key = f.getString(0)
      val value = f.getString(1)
      (key,value)
    }).reduceByKey(_+" "+_).map(_._2)   //I have to use distinct on _.2 but when i am using distinct the we can't read entities properly
    
    clusters.foreach(println)
    println("Total Number of Clusters are: ", clusters.count())   
    entity_dataset.unpersist()
    
    var l = List[(String, Integer)]()  
    
    val sil = clusters.map(f => {
      j = j + 1
      val arr = f.split(" ")
      val len = arr.length        
      for(i <- 0 to len-1) {
        l = l.::(arr(i),j)
      }   
      (l)
    })

    val silhou = sil.flatMap(f=>f).toDF("entities","prediction").distinct()
    
    var silhoutte_input = silhou.join(featuredData_Df,"entities").drop("attributes" , "words", "filtered_words")
    
    silhoutte_input = silhoutte_input.repartition(4).persist 
    
    println("calculating LSH Silhoutte:")
    import org.apache.spark.ml.evaluation.ClusteringEvaluator
    val evaluator = new ClusteringEvaluator().setPredictionCol("prediction").setFeaturesCol("features").setMetricName("silhouette")
      
    val silhouette = evaluator.evaluate(silhoutte_input)
    println(s"Silhouette for LSH = $silhouette")




    
    /*
    //trying graphframe approach - start
    println("graphframes: ")
     import spark.implicits._
    val edgeDF = combined_.toDF("src","dst")
    val list_col_1 = edgeDF.rdd.map(f=>f.get(0).toString())//.collect().toList
    val list_col_2 = edgeDF.rdd.map(f=>f.get(1).toString())//.collect().toList
    val entities_for_vertex = list_col_1.union(list_col_2).distinct 
    
    val vertexDF = entities_for_vertex.toDF("id")
    val g = GraphFrame(vertexDF, edgeDF)
    
    spark.sparkContext.setCheckpointDir("/home/pratik/Downloads/saprk-checkpoint")
    val connected_components = g.connectedComponents.run()
    //connected_components.show(false)  
    connected_components.select("component").rdd.distinct().foreach(println)
    //trying graphframe approach - end
    */
    
  /* val combined_ = entity_dataset.rdd.map(f => {
      (f.get(0).toString(),f.get(1).toString())
    })*/
    
    
// Making clusters using connected component of a Graph - start
    //ClusterFormation.run(spark,combined_, featuredData_Df)
// Making clusters using connected component of a Graph - end
    
    }

  
  }
