package com.aws.s3;

import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

// Executor method to utilize the various functions defined in the Utilities
public class Executor {
  public static void main(String[] args) throws URISyntaxException, IOException {
	  createBucket();
  }

  static void createBucket() {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    System.out.println(utilities.createBucket(bucketName));
  }

  static void deleteBucket() {
	Utilities utilities = new Utilities();
	String bucketName = "BUCKET_NAME"; // bucket name
	utilities.deleteBucket(bucketName);
	 
  }

  static void listExistingBuckets() {
    Utilities utilities = new Utilities();
    System.out.println(utilities.listExistingS3Buckets());
  }

  static void listExistingS3BucketContents() {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    System.out.println(utilities.listExistingS3BucketContents(bucketName));
  }

  static void uploadObject() throws URISyntaxException {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    String key = "File01.txt";
    Path localFilePath = Paths.get("C:/S3-Code/Upload/File01.txt");
    System.out.println(utilities.uploadObject(bucketName, key, localFilePath));
  }

  static void uploadObjectWithMetadata() throws URISyntaxException {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    String key = "File02.txt";
    Path localFilePath = Paths.get("C:/S3-Code/Upload/File02.txt");
    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put("Project", "HRMS");
    metadataMap.put("Owner", "Ahmad");
    System.out.println(utilities.uploadObjectWithMetadata(bucketName, key, metadataMap, localFilePath));
  }

  static void updateMetadata() {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    String key = "File01.txt";
    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put("Project", "CRM");
    System.out.println(utilities.updateMetadata(bucketName, key, metadataMap));
  }

  static void getObject() throws IOException {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    String key = "File01.txt";
    ResponseInputStream<GetObjectResponse> responseInputStream = utilities.getObject(bucketName, key);
    System.out.println(IOUtils.toString(responseInputStream, StandardCharsets.UTF_8));
    System.out.println(responseInputStream.response().metadata());
  }

  static void getPresignedUrl() {
    Utilities utilities = new Utilities();
    String bucketName = "BUCKET_NAME"; // bucket name
    String key = "File02.txt";
    System.out.println(utilities.getPresignedUrl(bucketName, key, 30));
  }

  static void getPresignedUrlData() throws IOException {
    Utilities utilities = new Utilities();
    URL url = new URL("PRE-SIGNED_URL");
    InputStream stream = utilities.getPresignedUrlData(url);
    System.out.println(IOUtils.toString(stream, StandardCharsets.UTF_8));
  }

}
