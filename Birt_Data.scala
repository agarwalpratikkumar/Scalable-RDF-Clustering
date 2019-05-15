package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Birt_Data {
  
  def run(spark: SparkSession) {
    combine_different_types(spark)
  
  }
  
  def combine_different_types(spark: SparkSession) {
    var data = spark.read.csv("src/main/resources/Employees.csv")    
    var column_name =  Seq("employeeNumber", "lastName", "firstName", "extension", "email" , "officeCode" , "reportsTo", "jobTitle")
    data = data.toDF(column_name: _*)
    
    var columns = data.columns    
    var data_rdd = data.rdd    
    var all_predicate = extract_predicates(data_rdd, columns)
   //var all_predicate = PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(data_rdd, columns)
        
    
    data = spark.read.csv("src/main/resources/Customers.csv")
    column_name =  Seq("customerNumber", "customerName", "contactLastName", "contactFirstName", "phone" , "addressLine1" , "addressLine2", "city", "state", "postalCode" ,"country" ,"salesRepEmployeeNumber", "creditLimit")
    data = data.toDF(column_name: _*)
    
    columns = data.columns    
    data_rdd = data.rdd    
    all_predicate = all_predicate.union(extract_predicates(data_rdd, columns))
    //all_predicate = all_predicate.union(PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(data_rdd, columns))
    
    data = spark.read.csv("src/main/resources/Offices.csv")
    column_name =  Seq("officeCode", "city", "phone" , "addressLine1" , "addressLine2" , "state" , "country", "postalCode", "territory")
    data = data.toDF(column_name: _*)
    
    columns = data.columns    
    data_rdd = data.rdd    
    all_predicate = all_predicate.union(extract_predicates(data_rdd, columns))
    //all_predicate = all_predicate.union(PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(data_rdd, columns))
    
    data = spark.read.csv("src/main/resources/OrderDetails.csv")
    column_name =  Seq("orderNumber", "productCode", "quantityOrdered" , "priceEach" , "orderLineNumber")
    data = data.toDF(column_name: _*)
    
    columns = data.columns    
    data_rdd= data.rdd    
    all_predicate = all_predicate.union(extract_predicates(data_rdd, columns))
    //all_predicate = all_predicate.union(PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(data_rdd, columns))
    
    /*data = spark.read.csv("src/main/resources/Orders.csv")
    column_name =  Seq("orderNumber", "orderDate", "requiredDate" , "shippedDate", "status", "comments", "customerNumber")
    data = data.toDF(column_name: _*)
    
    columns = data.columns    
    data_rdd= data.rdd    
    all_predicate = all_predicate.union(extract_predicates(data_rdd, columns))*/
    //all_predicate = all_predicate.union(PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(data_rdd, columns))
    
    /*data = spark.read.csv("src/main/resources/Payments.csv")
    column_name =  Seq("customerNumber", "checkNumber", "paymentDate" , "amount")
    data = data.toDF(column_name: _*)
    
    columns = data.columns    
    data_rdd= data.rdd    
    //all_predicate = all_predicate.union(extract_predicates(data_rdd, columns))
    all_predicate = all_predicate.union(PreprocessingDataFrame_csvjson.extractedentity_AllPredicates(data_rdd, columns))*/
    
   /* data = spark.read.csv("src/main/resources/Products.csv")
    column_name =  Seq("productCode", "productName", "productLine" , "productScale", "productVendor", "productDescription", "quantityInStock", "buyPrice", "MSRP")
    data = data.toDF(column_name: _*)
    
    columns = data.columns    
    data_rdd= data.rdd    
    all_predicate = all_predicate.union(extract_predicates(data_rdd, columns))*/
    
    
    LSHoperation.run_dirty(spark, all_predicate)
    
    
    
    
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