import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  UploadPartCommand,
} from "@aws-sdk/client-s3";
import archiver from "archiver";
import express from "express";
import { PassThrough } from "stream";

const app = express();
const s3Client = new S3Client({
  region: process.env.REGION || "ap-south-1",
  credentials: {
    accessKeyId: process.env.ACCESS_KEY_ID as string,
    secretAccessKey: process.env.SECRET_ACCESS_KEY as string,
  },
});
const port = process.env.PORT || 3000;

app.get("/folders", async (req, res) => {
  const sourceBucket = process.env.BUCKET_NAME as string;
  const sourceKey = "test-data/";
  const destinationBucket = process.env.BUCKET_NAME as string;
  const destinationKey = `zip-outputs/zip-output-${Date.now()}.zip`;

  const objects = [];
  let isTruncated = true;

  try {
    while (isTruncated) {
      const listCommand = new ListObjectsV2Command({
        Bucket: sourceBucket,
        Prefix: sourceKey,
        MaxKeys: 1000,
      });

      const response = await s3Client.send(listCommand);
      response && response.Contents && objects.push(...response.Contents);
      isTruncated = response.IsTruncated ?? false;
    }
  } catch (error) {
    console.log(`Error while listing objects: ${error}`);
    res.status(500).json({
      status: "error",
      message: "There was an error while listing objects",
    });
  }

  if (objects.length === 0) {
    return res.status(400).json({
      status: "error",
      message: "No objects found",
    });
  }

  const archive = archiver("zip", {
    zlib: { level: 9 },
  });
  const passThrough = new PassThrough();
  archive.pipe(passThrough);

  const streamObjectSize = 1024 * 1024;
  const partSize = streamObjectSize * 5;

  const uploadBodyParts: Buffer[] = [];
  let buffer: Buffer[] = [];

  const createMultipartCommand = new CreateMultipartUploadCommand({
    Bucket: destinationBucket,
    Key: destinationKey,
  });

  const { UploadId: uploadId } = await s3Client.send(createMultipartCommand);

  if (!uploadId) {
    return res.status(500).json({
      status: "error",
      message: "There was an error while creating multipart upload",
    });
  }

  passThrough.on("data", async (chunk: Buffer) => {
    buffer.push(chunk);

    const currentBufferSize = buffer.reduce(
      (acc, curr) => acc + curr.length,
      0
    );

    if (currentBufferSize >= partSize) {
      const partBody = Buffer.concat(buffer);
      buffer = [];
      uploadBodyParts.push(partBody);
    }
  });

  passThrough.on("end", async () => {
    if (uploadBodyParts.length > 0) {
      console.log(
        `Stream ended. Finalizing multipart for upload ID: ${uploadId}`
      );
      if (buffer.length > 0) {
        console.log(
          `Buffer not empty with ${buffer.length} bytes. Uploading remaining buffer...`
        );
        const partBody = Buffer.concat(buffer);
        uploadBodyParts.push(partBody);
      }

      const uploadPromises = uploadBodyParts.map((partBody, index) => {
        const uploadPartCommand = new UploadPartCommand({
          Bucket: destinationBucket,
          Key: destinationKey,
          PartNumber: index + 1,
          UploadId: uploadId,
          Body: partBody,
        });
        return s3Client.send(uploadPartCommand).then(({ ETag: etag }) => {
          return {
            PartNumber: index + 1,
            ETag: etag,
          };
        });
      });

      const parts = await Promise.all(uploadPromises);

      const completeMultipartCommand = new CompleteMultipartUploadCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        MultipartUpload: {
          Parts: parts,
        },
        UploadId: uploadId,
      });
      await s3Client.send(completeMultipartCommand);
      console.log(`Multipart upload completed`);
    } else {
      console.log(`No parts found. Uploading entire object.`);

      const cancelMultipartCommand = new AbortMultipartUploadCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        UploadId: uploadId,
      });

      await s3Client.send(cancelMultipartCommand);
      console.log(`Multipart upload aborted`);

      const uploadCommand = new PutObjectCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        Body: Buffer.concat(buffer),
      });
      await s3Client.send(uploadCommand);

      console.log(`Object uploaded to destination bucket`);
    }

    console.log(`Zip file created at destination bucket`);
  });

  for (const file of objects) {
    if (!file.Key) continue;
    console.log(`Processing file: ${file.Key}`);
    let fileRangeAndLength = {
      start: -1,
      end: -1,
      length: -1,
    };

    while (fileRangeAndLength.end !== fileRangeAndLength.length - 1) {
      const { end } = fileRangeAndLength;
      const nextRange = {
        start: end + 1,
        end: end + streamObjectSize,
      };

      const getObjectCommand = new GetObjectCommand({
        Bucket: sourceBucket,
        Key: file.Key,
        Range: `bytes=${nextRange.start}-${nextRange.end}`,
      });

      const response = await s3Client.send(getObjectCommand);
      if (!response || !response.ContentRange || !response.Body) continue;

      const body = await response.Body.transformToByteArray();

      archive.append(Buffer.from(body), {
        name: file.Key.substring(file.Key.lastIndexOf("/") + 1),
      });

      const [range, length] = response.ContentRange.split("/") as [
        string,
        string
      ];
      const [newStart, newEnd] = range.split("-") as [string, string];
      fileRangeAndLength = {
        start: parseInt(newStart),
        end: parseInt(newEnd),
        length: parseInt(length),
      };
    }
    console.log(`Processed file: ${file.Key}`);
  }

  await archive.finalize();

  console.log("Archive created successfully");

  res.status(200).json({
    status: "success",
    message: "Files processed successfully",
  });
});

app.listen(port, () => {
  console.log(`Application listening on port ${port}`);
});
