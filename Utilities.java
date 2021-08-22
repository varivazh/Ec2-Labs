package com.aws.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utilities {
  private final S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
  private final S3Presigner presigner = S3Presigner.create();


  // Retrieves the list of existing buckets in s3
  public List<Bucket> listExistingS3Buckets() {
    return s3Client.listBuckets().buckets();
  }

  // Retrieves the contents of the existing bucket
  public List<S3Object> listExistingS3BucketContents(String bucketName) {
    List<S3Object> result = new ArrayList<>();
    ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucketName).build();
    ListObjectsV2Response response;
    while (true) {
      response = s3Client.listObjectsV2(req);
      result.addAll(response.contents());
      if (!response.isTruncated()) {
        break;
      }
      req = ListObjectsV2Request.builder().bucket(bucketName).continuationToken(response.nextContinuationToken()).build();
    }
    return result;
  }

  // creates a new bucket with the given name
  // throws error if their exists bucket with same name in aws
  public CreateBucketResponse createBucket(String bucketName) {
	  List<Bucket> existingBucketNames = listExistingS3Buckets();
	  for (Bucket bucket :
	          existingBucketNames) {
	    if (bucket.name().equals(bucketName)) {
	      throw new RuntimeException(String.format("The bucket %s already exists", bucketName));
	    }
	  }
	  try {
      CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
              .bucket(bucketName)
              .build();
      return s3Client.createBucket(bucketRequest);
    } catch (S3Exception e) {
    	if (e.awsErrorDetails().errorMessage().contains("The authorization header is malformed")){
            throw new RuntimeException(String.format("Bucket exists already with the name %s", bucketName));
          }
  	    throw new RuntimeException(e.awsErrorDetails().errorMessage());
    }
  }

  public void deleteBucket(String bucketName) {
    try {
      // To delete a bucket, all the objects in the bucket must be deleted first
      ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
      ListObjectsV2Response listObjectsV2Response;

      do {
        listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
        for (S3Object s3Object : listObjectsV2Response.contents()) {
          s3Client.deleteObject(DeleteObjectRequest.builder()
                  .bucket(bucketName)
                  .key(s3Object.key())
                  .build());
        }

        listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName)
                .continuationToken(listObjectsV2Response.nextContinuationToken())
                .build();

      } while (listObjectsV2Response.isTruncated());

      DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
      s3Client.deleteBucket(deleteBucketRequest);

    } catch (S3Exception e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }

  public PutObjectResponse uploadObject(String bucketName, String key, RequestBody requestBody) {
    return s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), requestBody);
  }

  public PutObjectResponse uploadObject(String bucketName, String key, Path filePath) {
    return s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), filePath);
  }

  public PutObjectResponse uploadObjectWithMetadata(String bucketName, String key, Map<String, String> metadata, RequestBody requestBody) {
    return s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).metadata(metadata).build(), requestBody);
  }

  public PutObjectResponse uploadObjectWithMetadata(String bucketName, String key, Map<String, String> metadata, Path filePath) {
    return s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).metadata(metadata).build(), filePath);
  }

  public CopyObjectResponse updateMetadata(String bucket, String key, Map<String, String> metadata) {
    CopyObjectRequest request = CopyObjectRequest.builder()
            .destinationBucket(bucket).destinationKey(key).copySource(bucket + "/" + key)
            .metadata(metadata)
            .metadataDirective("REPLACE").build();
    return s3Client.copyObject(request);
  }

  public ResponseInputStream<GetObjectResponse> getObject(String bucket, String key) {
    return s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
  }

  // returns presigned url for a given object
  public URL getPresignedUrl(String bucket, String key, long durationInMinutes) {
    GetObjectRequest getObjectRequest =
            GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
    GetObjectPresignRequest getObjectPresignRequest =
            GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(durationInMinutes))
                    .getObjectRequest(getObjectRequest)
                    .build();
    PresignedGetObjectRequest presignedGetObjectRequest =
            presigner.presignGetObject(getObjectPresignRequest);
    System.out.println("Presigned URL: " + presignedGetObjectRequest.url());
    presigner.close();
    return presignedGetObjectRequest.url();
  }

  // returns a map consisting of of presigned url for given objects
  public Map<String, URL> getPresignedUrl(String bucket, List<String> keys, long durationInMinutes) {
    Map<String, URL> res = new HashMap<>();
    for (String key : keys) {
      res.put(key, getPresignedUrl(bucket, key, durationInMinutes));
    }
    return res;
  }

  // returns stream, which need to be parsed respectively
  public InputStream getPresignedUrlData(URL url) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    return connection.getInputStream();
  }

}