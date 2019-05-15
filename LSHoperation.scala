package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ Tokenizer, HashingTF }
import org.apache.spark.ml.feature.CountVectorizer
//import org.dmg.pmml.False
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.util.regex._
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.calcite.rel.core.Intersect



object LSHoperation {
  
  var Arr_val:ArrayBuffer[Set[List[String]]] = _
  
  def run(spark: SparkSession, extractedEntity_1:RDD[(String,String)], extractedEntity_2: RDD[(String,String)], vocab_size_entity1: Int, vocab_size_entity2: Int) = {
    val extractedEntity = extractedEntity_1.union(extractedEntity_2).distinct()
    println("entity1"+extractedEntity_1.count())
    println("entity2"+extractedEntity_2.count())
    //run_dirty(spark,extractedEntity)
    val (featuredData_Df1: DataFrame, featuredData_Df2: DataFrame) = vectoriseText_clean(spark, extractedEntity_1, extractedEntity_2)
    //val featuredData_Df2: DataFrame = vectoriseText(spark, extractedEntity_2,vocab_size_entity2)
    //println(vocab_size_entity1)
    //println(vocab_size_entity2)
    
    val (model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame) = minHashLSH_clean(featuredData_Df1,featuredData_Df2)
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df1,transformedData_Df2)
    //dataset.show(false)
    //matchentities(spark,dataset)
  }
  def run_dirty(spark: SparkSession, extractedEntity: RDD[(String, String)]) = {
    //extractedEntity_NoPredicates.foreach(println)
    //extractedEntity_FilterPredicates.foreach(println)
    //extractedEntity_AllPredicates.foreach(println)
    //extractedEntity_SO.foreach(println)
    //extractedEntity_SPO.foreach(println)
    val vocab_size = 3231
    val featuredData_Df: DataFrame = vectoriseText(spark, extractedEntity, vocab_size)
    featuredData_Df.show(false)
    //println(featuredData_Df.count())
    //featuredData_Df.coalesce(1).rdd.foreach(println)
    //K_Means.run(spark, featuredData_Df)
    val (model: MinHashLSHModel, transformedData_Df: DataFrame) = minHashLSH(featuredData_Df)
    //transformedData_Df.show(false)
    //transformedData_Df.select("Features").show(false)
    
    val dataset: Dataset[_] = approxSimilarityJoin(model, transformedData_Df)
    println("approx sim join: ")
    //dataset.show(false)
    
    import org.apache.spark.ml.linalg.Vectors
    val x = featuredData_Df.select("Features")
        
    val x_rdd = x.rdd
    
    val xx = x_rdd.map(f=>{f.toString()})
   
    val t= xx.map(f=>{f.stripSuffix("]").stripPrefix("[")})    
    
    //val key = (414,[0,1,2,3,4,6,7,8,9,10,11,12,13,14,15,17],[10.0,18.0,5.0,4.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])
  
    
    val key = Vectors.sparse(414, Seq((0, 10.0), (1, 18.0), (2, 5.0), (3, 4.0), (4, 1.0), (5, 1.0), (6, 1.0), (7, 1.0), (8, 1.0), (9, 1.0), (10, 1.0), (11, 1.0), (12, 1.0), (13, 1.0), (14, 1.0), (15, 1.0), (17, 1.0)))
    
    
    /*val data = t.map(f=>Vectors.dense(f.split(",").map(f=>f.toDouble))).cache()
    println("approx near neigbr: ")
    val k = 0
    
    val ff = data.take(1)
    print("key is: ")
    ff.foreach(println)
    print(ff.last)*/
    model.approxNearestNeighbors(transformedData_Df.drop("attributes", "words", "filtered_words"), key, 5).show(false)
    

    
   

    
    
    
  // matchentities(spark,dataset, featuredData_Df)
    
    //val blocks:RelationalGroupedDataset = dataset.groupBy("datasetA.entities")
    
  }
  
  
  def remove_stopwords(tokenizedData_Df: DataFrame): DataFrame = {
    val remover =  new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val removed_df = remover.transform(tokenizedData_Df)
    //removed_df.withColumn("filtered_words", when(col("filtered_words").contains(", null"), "") otherwise(col("filtered_words")))
    //removed_df.show(false)
    return remover.transform(tokenizedData_Df)
  }
  def vectoriseText(spark: SparkSession, entities: RDD[(String, String)], vocab_size:Int): DataFrame = {
    // Using HashingTf for converting text to vector format

    /*
     *
     *Here we create a dataframe on the extracted entities naming the two columns("entities", "attributes") in the entityProfile_Df
     * Then we are tokenizing the column "attributes" i.e. breaking the sentences or phrases to words and we add a new column words to our new dataFrame tokenizedData_Df
     * Now, we vectoise the text by either using HashingTf or CountVectorizer method.
     * So we create featuredData_hashedDf by HashingTf method, setting our setNumFeatures to 20 i.e. this means that it would probably encounter 20 different terms/words in the words column
     * We try to avoid collisions by keeping this value high.
     * Also, we have created the featuredData_cvDf using the CountVectorizer method, setting our setVocabSize to 20. This means that in our dictionary we will be adding approximately terms<=20
     *
     */
    val entityProfile_Df = spark.createDataFrame(entities).toDF("entities", "attributes")
    //entityProfile_Df.show(false)
    entityProfile_Df.select("attributes").distinct()
    val tokenizer = new Tokenizer().setInputCol("attributes").setOutputCol("words")
    val tokenizedData_Df = tokenizer.transform(entityProfile_Df)
    //tokenizedData_Df.show(false)
    val cleanData_Df = remove_stopwords(tokenizedData_Df).distinct
    val cleandfrdd = cleanData_Df.select("filtered_words").distinct.rdd
    val vocab_size = calculate_Vocabsize(cleandfrdd)
    println("vocab_size:"+vocab_size)
    //cleanData_Df.select("entities", "filtered_words").show(false)
    //val vocabSize = 1000000
    //cleanData_Df.select("filtered_words").rdd.foreach(println)
    val hashingTf = new HashingTF().setInputCol("filtered_words").setOutputCol("raw_Features").setNumFeatures(vocab_size)
    val featuredData_hashedDf = hashingTf.transform(cleanData_Df)
    //featuredData_hashedDf.show(false)
    val idf = new IDF().setInputCol("raw_Features").setOutputCol("features")
    val idfModel = idf.fit(featuredData_hashedDf)
    val rescaledHashedData = idfModel.transform(featuredData_hashedDf)
    //rescaledHashedData.show(false)
    val cvModel = new CountVectorizer().setInputCol("filtered_words").setOutputCol("features").setVocabSize(Math.round(vocab_size).toInt).fit(cleanData_Df)
    val featuredData_cvDf = cvModel.transform(cleanData_Df)
    //featuredData_cvDf.select("entities","Features").show(false)

    /*val idf = new IDF().setInputCol("Features").setOutputCol("scaledFeatures")
    val idfModel = idf.fit(featuredData_cvDf) //scaling the features
    val rescaledData = idfModel.transform(featuredData_cvDf) */
    //return rescaledHashedData
    return featuredData_cvDf
  }
  
  def calculate_Vocabsize(cleandfrdd:RDD[Row]): Int = {
    var s = cleandfrdd.map(_.mkString.replace("WrappedArray(", "").replace("(", "").replace(")", ", ")).reduce(_+_).split(", ").toSet  
    s-=("-","/")
    //s.foreach(println)
    return s.size
  }
  def vectoriseText_clean(spark: SparkSession, entities1: RDD[(String, String)], entities2: RDD[(String,String)]): (DataFrame,DataFrame) = {
    val entityProfile_Df1 = spark.createDataFrame(entities1).toDF("entities", "attributes")
    val entityProfileDf2 = spark.createDataFrame(entities2).toDF("entities", "attributes")
    //entityProfile_Df.show(false)
    val tokenizer = new Tokenizer().setInputCol("attributes").setOutputCol("words")
    val tokenizedData_Df1 = tokenizer.transform(entityProfile_Df1)
    val tokenizedData_Df2 =  tokenizer.transform(entityProfileDf2)
    //tokenizedData_Df1.show(false)
    val cleanData_Df1 = remove_stopwords(tokenizedData_Df1)
    val cleanData_Df2 =  remove_stopwords(tokenizedData_Df2)
    val cleandfrdd = cleanData_Df1.union(cleanData_Df2).select("filtered_words").distinct.rdd    
    val vocab_size = calculate_Vocabsize(cleandfrdd)
    println("vocab_size:"+vocab_size)
    val cleanData_DF = cleanData_Df1.union(cleanData_Df2).distinct()
    
    val hashingTf = new HashingTF().setInputCol("filtered_words").setOutputCol("raw_Features").setNumFeatures(vocab_size)
    val featuredData_hashedDf1 = hashingTf.transform(cleanData_Df1)
    val featuredData_hashedDf2 = hashingTf.transform(cleanData_Df2)
    val idf = new IDF().setInputCol("raw_Features").setOutputCol("features")
    val idfModel = idf.fit(featuredData_hashedDf2.union(featuredData_hashedDf1))
    val rescaledHashedData1 = idfModel.transform(featuredData_hashedDf2)
    val rescaledHashedData2 = idfModel.transform(featuredData_hashedDf1)
    //featuredData_hashedDf.show(false)
    val cvModel = new CountVectorizer().setInputCol("filtered_words").setOutputCol("features").setVocabSize(Math.round(0.65*vocab_size).toInt).fit(cleanData_DF) //Math.round(0.85*(1368+3231)).toInt
    val featuredData_cvDf1 = cvModel.transform(cleanData_Df1)
    val featuredData_cvDf2 = cvModel.transform(cleanData_Df2)
    //featuredData_cvDf.select("entities","Features").show(false)

    /*val idf = new IDF().setInputCol("Features").setOutputCol("scaledFeatures")
    val idfModel = idf.fit(featuredData_cvDf) //scaling the features
    val rescaledData = idfModel.transform(featuredData_cvDf) */
    return (featuredData_cvDf1, featuredData_cvDf2)
    //return (rescaledHashedData1, rescaledHashedData2)
    //return (featuredData_hashedDf1, featuredData_hashedDf2)
  }

  def minHashLSH(featuredData_Df: DataFrame): (MinHashLSHModel, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(5).setInputCol("features").setOutputCol("HashedValues")
    val model = mh.fit(featuredData_Df)
    val transformedData_Df = model.transform(featuredData_Df)
    //transformedData_Df.show(false)
    return (model, transformedData_Df)
  }
  
  def minHashLSH_clean(featuredData_Df1: DataFrame, featuredData_Df2: DataFrame): (MinHashLSHModel, DataFrame, DataFrame) = {
    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("HashedValues")
    val featuredData_DF =  featuredData_Df1.union(featuredData_Df2).distinct()
    var model = mh.fit(featuredData_DF)
    //model = mh.fit(featuredData_Df2)
    val transformedData_Df1 = model.transform(featuredData_Df1)
    val transformedData_Df2 = model.transform(featuredData_Df2)
    //println(transformedData_Df1.count())
    //println(transformedData_Df2.count())
    return (model, transformedData_Df1, transformedData_Df2)
  }

  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df: DataFrame): Dataset[_] = {
    val threshold = 0.45
    return model.approxSimilarityJoin(transformedData_Df, transformedData_Df, threshold)
  //0.80 - NO Predicates
  //0.75 - FILTER Predicates
  //0.75 - ALL Predicates
  //0.75 - SPO
  }
  
  def approxSimilarityJoin(model: MinHashLSHModel, transformedData_Df1: DataFrame, transformedData_Df2: DataFrame): Dataset[_] = {
   val threshold = 0.447
   return model.approxSimilarityJoin(transformedData_Df1, transformedData_Df2, threshold)
  }
  
  /*def matchentities(spark:SparkSession,dataset:Dataset[_]) = {
    println("entered matched entities: ")
    if(!dataset.isEmpty)
    {
    //println(dataset.count())
    val refined_entities_dataset = dataset
    //.filter("datasetA.entities != datasetB.entities")
    .select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2"),col("distCol"))
    
    //refined_entities_dataset.show(false)
    
    //refined_entities_dataset.coalesce(1).rdd.foreach(println)
     
    
    
    val df_of_ground = spark.read
         .format("csv")
         .option("header", "true") //first line in file has headers
         .option("mode", "DROPMALFORMED")
         .load("src/main/resources/DBLP-ACM_perfectMapping.csv")    //change name according to dataset.
    
    df_of_ground.show(false)
   
    import org.apache.spark.sql.functions.struct
    val teacher = df_of_ground.withColumn("new", struct(df_of_ground("idDBLP"), df_of_ground("idACM"))).select("new").rdd    //idACM  in first
    val predicted = refined_entities_dataset.withColumn("new", struct(refined_entities_dataset("entity1"), refined_entities_dataset("entity2"))).select("new").rdd
    //teacher.foreach(println)
    //predicted.foreach(println)
    val ss = predicted.intersection(teacher).collect()
    println(predicted.count, ss.length)
  }}*/
  
  def matchentities(spark:SparkSession,dataset:Dataset[_], featuredData_Df:DataFrame) = {
    println("entered matched entities: ")
    //println(dataset.count())
    //dataset.show(false)
    if(!dataset.isEmpty)
    {
    //println(dataset.count())
    val refined_entities_dataset = dataset
    //.filter("datasetA.entities != datasetB.entities")
    .select(col("datasetA.entities").alias("entity1"),col("datasetB.entities").alias("entity2"),col("distCol"))
    
    //refined_entities_dataset.show(false)
    
    //refined_entities_dataset.coalesce(1).rdd.foreach(println)
    
    //import org.apache.spark.ml.feature.SQLTransformer
    //val sqlTrans = new SQLTransformer().setStatement("SELECT * FROM __THIS__ GROUP BY datasetA.entities")
    //sqlTrans.transform(refined_entities_dataset).show(false)
    
    
    //refined_entities_dataset.withColumn("type", split(col("entity1"), "_").getItem(1)).groupBy("entity1").count().distinct().show(false)
    //refined_entities_dataset.groupBy("entity1").count().distinct().show(false)
    
    val combined = refined_entities_dataset.rdd.map(f => {
      val key = f.getString(0)
      val value = f.getString(1)
      (key,value)
    }).reduceByKey(_+","+_).distinct()
    
    //var set_ = Set[List[String]]()
    
    val li = combined.map(f => {
      f._2.split(",").toList.sorted
      })
    
      //var on = li.randomSplit(Array(0.70))
      //print(on.length)

      
      val set1= li.collect().toSet
      set1.foreach(println)    
   
     /* val new__ = set1.grouped(set1.size/2).toSet
      
      new__.foreach(println)
      val is_arr = new__.splitAt(1)
      //println(is_arr._1.size, is_arr._2.size)
      var br = is_arr._1.flatten
      var dd = is_arr._2.flatten
      br.foreach(println)
      println("++++++++++++++++++++++++++++++++++++++")
      dd.foreach(println)
println("**********************************************************")
      val broadcastVar = spark.sparkContext.broadcast(dd)
      println(broadcastVar.value)*/
      
      /*var new_list = List(" ").toArray
      var i = 0
      set1.foreach(f => {
        if(f.intersect(new_list).length > f.length/2)
          new_list(i).union(f)
        else
          
      })*/
      
      println("Number of clusters formed : " + set1.size)
      
      val entities_ = featuredData_Df.select("entities")
      
      import org.apache.spark.broadcast.Broadcast
      import spark.implicits._

      def udf_check(words: Broadcast[scala.collection.immutable.Set[String]]) = {
        udf {(s: String) => words.value.exists(s.contains(_))}
      }
      var i = 0
      
      import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
      import org.apache.spark.sql.Row
      val schema = StructType(
          StructField("entities", StringType, true) ::
          StructField("prediction", IntegerType, true) :: Nil)
        var initialDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    
      set1.map(f => {
                
        val inter = f.toDF.intersect(entities_).toDF("entities")
        val clu = inter.withColumn("prediction", lit(i))
        
        /*val result = inter.withColumn("word_check", udf_check(spark.sparkContext.broadcast(f.toSet))($"entities"))
        result.show(false)
       
        implicit def bool2int(b:Boolean) = if (b) i else 0
        val bool2int_udf = udf(bool2int _)
        result.select("word_check","entities").withColumn("cluster_number",bool2int_udf($"word_check")).show(false)
        */
        
        i += 1

        //featuredData_Df.join(clu , "entities").show()
        
        initialDF = initialDF.union(clu)
        

      })
    //println(initialDF.count())
    //initialDF.coalesce(1).rdd.foreach(println)
    var silhoutte_input = featuredData_Df.join(initialDF, "entities").drop("attributes" , "words", "filtered_words")
    silhoutte_input.createOrReplaceTempView("wordcount")
    silhoutte_input = spark.sqlContext.sql("select row_number() over (order by entities) as label,entities, features, prediction from wordcount")

    //silhoutte_input.coalesce(1).rdd.foreach(println)
    calculate_silhoutte(silhoutte_input)
    
  
    /*val df_of_ground = spark.read
         .format("csv")
         .option("header", "true") //first line in file has headers
         .option("mode", "DROPMALFORMED")
         .load("src/main/resources/DBLP-Scholar_perfectMapping.csv")    //change name according to dataset.
    
    df_of_ground.show(false)
   
    import org.apache.spark.sql.functions.struct
    val teacher = df_of_ground.withColumn("new", struct(df_of_ground("idScholar"), df_of_ground("idDBLP"))).select("new").rdd    //idACM  in first
    val predicted = refined_entities_dataset.withColumn("new", struct(refined_entities_dataset("entity1"), refined_entities_dataset("entity2"))).select("new").rdd

    val ss = predicted.intersection(teacher).collect()
    println(ss.length)*/
    //val refined_output_rdd= refined_entities_dataset.rdd.map(_.toString().replace("[", "").replace("]", ""))
  //  refined_output_rdd.saveAsTextFile("src/main/resources/output")
   }
   //val x = refined_entities_dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK) 
    /* val refined_entities_RDDA = refined_entities_dataset.rdd.map(f => {
      val key = f.getString(0)
      val value = f.getString(1)
      (key, value)
    }).reduceByKey(_+","+_)
    .map(f=>{
      if(f._1<f._2)
        (f._1,f._2)
      else
        (f._2,f._1)
    })
    .distinct()
    .foreach(println)
     val initialSet3 = mutable.Set.empty[String]
    //initialSet3.foreach(println)
    val addToset3  = (s: mutable.Set[String], v: String) => s += v
    val mergePartitiononSet3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val x1 = refined_entities_dataset.repartition(400).persist(StorageLevel.MEMORY_AND_DISK) 
    val x1Map = x1.rdd.map(row => {
      val id = row.getString(0)
      val value = row.getString(1)
      (id, value)
    })
    //x1Map.foreach(println)

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    //println(addToSet3)
    val mergePartitionSets3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    //println(mergePartitionSets3)
    val uniqueByKey3 = x1Map.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)
    //uniqueByKey3.foreach(println)
  

    val k = uniqueByKey3.map(f => ((f._1, (f._2 += (f._1)).toSet)))
    //k.foreach(println)
    val partitioner = new HashPartitioner(500) //500 working fine
    val mapSubWithTriplesPart = mapSubWithTriples.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK) 

    val ys = k.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK) 
    val joinSimSubTriples2 = ys.join(mapSubWithTriplesPart)

    val clusterOfSubjects = joinSimSubTriples2.map({
      case (s, (iter, iter1)) => ((iter).toSet, iter1) 
    })*/
  }
  
  def calculate_silhoutte(silhoutte_input: DataFrame) {
    println("calculating LSH Silhoutte:")
    val predictions = silhoutte_input.drop("entities")
    predictions.show(false)
    
      import org.apache.spark.ml.evaluation.ClusteringEvaluator
      val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance for LSH = $silhouette")
    
    calculate_kmeans_silhoutee(predictions)
    
  }
  
  def calculate_kmeans_silhoutee(prediction_data:DataFrame) {
    println("calculating kmeans Silhoutte: ")
    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.ml.evaluation.ClusteringEvaluator
    
    val daataset = prediction_data.drop("prediction")
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(daataset)
    val predictions = model.transform(daataset)
    predictions.show(false)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance for KMeans= $silhouette")
    
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
  
}
