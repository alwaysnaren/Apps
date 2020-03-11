package com.mastercard.ess.schema

import java.net.URI

import com.databricks.spark.avro.SchemaConverters
import com.mastercard.ess.UDF
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.mutable.Map

/**
 * Created by e043967 on 3/18/2019.
 */
object SchemaRegistry {

  var hdfsUri: String = null;
  var schemaDirectory: String = null
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val schemaMap : Map[String,SchemaInfo] = Map[String,SchemaInfo]()
  var sparkSession: SparkSession = null

  def apply(uri: String,sd: String,ss: SparkSession) = {
    hdfsUri = uri
    schemaDirectory = sd
    this.sparkSession = ss
  }

  /*def addSchema(topic:String,alias:String, schema: Schema)  = {

    val structType: StructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    val schemaInfo = new SchemaInfo(schema,structType)
    schemaMap.put(topic,schemaInfo)
    val deserializeData = udf(UDF.deserializer(schema.toString), DataTypes.createStructType(structType.fields))
    sparkSession.udf.register("deserialize_" + alias, deserializeData)

  }*/


  def addSchema(topic:String,alias: String,schemaName: String) : SchemaInfo = {
        try
        {
          val hdfsConfigs = new Configuration()
          val parser = new Schema.Parser()

          val fs = FileSystem.get(new URI(hdfsUri), hdfsConfigs)
          val filePath = new Path(schemaDirectory + "/" + schemaName)

          val exists = fs.exists(filePath)

          if(!exists) {
            logger.error("File schema not found, cannot deserialize. Path: " + filePath.toString)
            return null
          }

          val input = fs.open(filePath)
          val schem = parser.parse(input)

          var structType:StructType = SchemaConverters.toSqlType(schem).dataType.asInstanceOf[StructType]
          val schemaInfo = new SchemaInfo(schem,structType)
          schemaMap.put(topic,schemaInfo)

          val deserializeData = udf(UDF.deserializer(schem.toString), DataTypes.createStructType(structType.fields))
          //TODO this string addition is in 2 places. Fix that
          sparkSession.udf.register("deserialize_" + alias, deserializeData)

          schemaInfo

        }catch {
          case e: Exception => return null
        }
  }

  def addSchema(topic:String,alias:String, schema: Schema)  = {

    val structType: StructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    val schemaInfo = new SchemaInfo(schema,structType)
    schemaMap.put(topic,schemaInfo)
    val deserializeData = udf(UDF.deserializer(schema.toString), DataTypes.createStructType(structType.fields))
    sparkSession.udf.register("deserialize_" + alias, deserializeData)

  }

  def getSchemaFields(topic: String) : Array[String] = {
    val schemaInfo : Option[SchemaInfo] = schemaMap.get(topic)
    if(!schemaInfo.isEmpty) {
      schemaInfo.get.structType.fieldNames
    }
    null
  }

  def retrieveSchema(topic: String) : SchemaInfo = {
    schemaMap.get(topic).get
  }
  
  def deserializer(schema: String) = (data: Array[Byte])=> {
    val parsertest = new Schema.Parser()
    val schemaUpdated = parsertest.parse(schema)
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schemaUpdated)
    val record = recordInjection.invert(data).get
    val myAvroType = SchemaConverters.toSqlType(record.getSchema).dataType
    val myAvroRecordConverter = MySchemaConversions.createConverterToSQL(record.getSchema, myAvroType)
    myAvroRecordConverter.apply(record)
  }


}
