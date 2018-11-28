import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

import com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}

import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}

import com.mongodb.hadoop.io.MongoUpdateWritable

