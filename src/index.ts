import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
  UploadPartCommand,
} from "@aws-sdk/client-s3";
import archiver from "archiver";
import express from "express";
import pLimit from "p-limit";
import { PassThrough } from "stream";
import { v4 as uuid } from "uuid";
import { genericInternalErrorResponse } from "./util";

const app = express();

app.use(express.json());

const s3Client = new S3Client({
  region: process.env.REGION || "ap-south-1",
  credentials: {
    accessKeyId: process.env.ACCESS_KEY_ID as string,
    secretAccessKey: process.env.SECRET_ACCESS_KEY as string,
  },
});
const port = process.env.PORT || 3000;

app.post("/folders", async (req, res) => {
  const { sourceKey, destinationKey } = req.body;

  if (!sourceKey || !destinationKey) {
    return res.status(400).json({
      status: "error",
      message: "Missing required parameters",
    });
  }

  const requestId = uuid();

  const sourceBucket = process.env.BUCKET_NAME as string;
  const destinationBucket = process.env.AWS_S3_ZIP_BUCKET_NAME as string;

  console.log(`FETCH-OBJECT::For request ${requestId} fetching objects`);

  try {
    const objects = [];
    let isTruncated = true;
    let continuationToken: string | undefined;

    while (isTruncated) {
      const listCommand = new ListObjectsV2Command({
        Bucket: sourceBucket,
        Prefix: sourceKey,
        MaxKeys: 1000,
        ...(continuationToken && { ContinuationToken: continuationToken }),
      });

      const response = await s3Client.send(listCommand);
      response && response.Contents && objects.push(...response.Contents);
      isTruncated = response.IsTruncated ?? false;
      continuationToken = response.NextContinuationToken;
    }

    const filteredObjects = objects.filter((object) => object.Size !== 0);

    console.log(
      `FETCH-OBJECT::For request ${requestId} found ${filteredObjects.length} objects`
    );

    if (filteredObjects.length === 0) {
      return res.status(400).json({
        requestId,
        status: "error",
        message: "No objects found",
      });
    }

    const archive = archiver("zip");
    const passThrough = new PassThrough();
    archive.pipe(passThrough);

    const partSize = 5 * 1024 * 1024;

    const parts: { PartNumber: number; ETag: string }[] = [];
    let buffer: Buffer[] = []; // held in memory until part size is reached

    const createMultipartCommand = new CreateMultipartUploadCommand({
      Bucket: destinationBucket,
      Key: destinationKey,
    });

    const { UploadId: uploadId } = await s3Client.send(createMultipartCommand);

    if (!uploadId) {
      return res.status(500).json(genericInternalErrorResponse(requestId));
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

        const uploadPartCommand = new UploadPartCommand({
          Bucket: destinationBucket,
          Key: destinationKey,
          PartNumber: parts.length + 1,
          UploadId: uploadId,
          Body: partBody,
        });

        passThrough.pause();

        const { ETag: etag } = await s3Client.send(uploadPartCommand);
        etag &&
          parts.push({
            PartNumber: parts.length + 1,
            ETag: etag,
          });
        console.log(
          `UPLOAD-PART::For request ${requestId} uploaded part ${
            parts.length + 1
          } with ETag: ${etag}`
        );
        passThrough.resume();
      }
    });

    passThrough.on("end", async () => {
      if (buffer.length > 0) {
        const partBody = Buffer.concat(buffer);

        const uploadPartCommand = new UploadPartCommand({
          Bucket: destinationBucket,
          Key: destinationKey,
          PartNumber: parts.length + 1,
          UploadId: uploadId,
          Body: partBody,
        });
        const { ETag: etag } = await s3Client.send(uploadPartCommand);
        console.log(
          `UPLOAD-PART::For request ${requestId} uploaded part ${
            parts.length + 1
          } with ETag: ${etag}`
        );
        etag &&
          parts.push({
            PartNumber: parts.length + 1,
            ETag: etag,
          });
      }

      console.log(
        `UPLOAD-PARTS::For request ${requestId} finishing upload with ${parts.length} parts`
      );

      const completeMultipartCommand = new CompleteMultipartUploadCommand({
        Bucket: destinationBucket,
        Key: destinationKey,
        MultipartUpload: {
          Parts: parts,
        },
        UploadId: uploadId,
      });
      await s3Client.send(completeMultipartCommand);

      console.log(`UPLOAD-PARTS::For request ${requestId} completed upload`);
    });

    const fetchFileLimit = pLimit(20); // limits to 20 concurrent requests, adjust as needed

    const filePromises = filteredObjects.map((file) =>
      fetchFileLimit(async () => {
        if (!file.Key) return;

        const getObjectCommand = new GetObjectCommand({
          Bucket: sourceBucket,
          Key: file.Key,
        });

        const response = await s3Client.send(getObjectCommand);
        console.log(
          `DOWNLOAD-FILE::For request ${requestId} downloaded ${file.Key}`
        );
        if (!response || !response.Body) return;
        const body = await response.Body.transformToByteArray();
        archive.append(Buffer.from(body), {
          name: file.Key
            ? file.Key.substring(file.Key.lastIndexOf("/") + 1)
            : "",
        });
      })
    );

    await Promise.all(filePromises);

    archive.finalize();

    console.log(`DOWNLOAD-FILE::For request ${requestId} completed download`);

    res.status(200).json({
      status: "success",
      message: "Files processed successfully",
    });
  } catch (error) {
    console.log(error);
  }
});

app.listen(port, () => {
  console.log(`Application listening on port ${port}`);
});
