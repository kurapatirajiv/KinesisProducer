package com.cloudwick.Kinesis


import java.nio.ByteBuffer

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest

import scala.io.Source
import scala.util.Random


/**
 * Generates random data and feeds the kinesis stream
 *
 * Reference : Spark Streaming / Producers / Kinesis Word Producer by Databricks
 *
 */


object KinesisStreamProducer {

  def main(args: Array[String]) {

    if (args.length != 5) {
      println("Usage : KinesisStreamProducer <AccessKey> <SecretKey> <StreamName> <EndPointURL> <TransferRate: NumOfRecords Per Seconds>")
      System.exit(-1)
    }
    val Array(awsAccessKeyId, awsSecretKey, kinesisStreamName, kinesisEndpointUrl, recordsPerSecond) = args
    val wordsPerRecord = 10
    val numSecondsToSend = 100
    // Running on S3
    val credentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey)
    val s3Client = new AmazonS3Client(credentials)
    val s3Object = s3Client.getObject(new GetObjectRequest("fakenewsinput", "MyRawNewsInputClean.txt"))
    val RecordsLines = Source.fromInputStream(s3Object.getObjectContent()).getLines.toList

    // Local Test Run
    // val RecordsLines = Source.fromFile("/Users/Rajiv/Desktop/Capstone Project/RealNewsInputVerOne.txt").getLines.toList

    // Create amazon client
    val kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey))
    kinesisClient.setEndpoint(kinesisEndpointUrl)

    println(s"Putting records onto stream '$kinesisStreamName' and endpoint '$kinesisEndpointUrl' at a rate of" +
      s" $recordsPerSecond records per second")

    // Function to generate data
    def generateData() = {
      val randomRecord = RecordsLines(Random.nextInt(RecordsLines.size))
      randomRecord
    }

    // Generate and send the data
    for (round <- 1 to numSecondsToSend) {
      for (recordNum <- 1 to recordsPerSecond.toInt) {
        val data = generateData()
        println(data)
        val partitionKey = s"partitionKey-$recordNum"
        val putRecordRequest = new PutRecordRequest().withStreamName(kinesisStreamName)
          .withPartitionKey(partitionKey)
          .withData(ByteBuffer.wrap(data.getBytes()))
        kinesisClient.putRecord(putRecordRequest)
      }
      Thread.sleep(1000) // Sleep for a second
      println(s"Sent $recordsPerSecond records with $wordsPerRecord words each")
    }
  }
}
