import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";

const s3Client = new S3Client({});

const client = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocument.from(client);

const tableName = process.env.SAMPLE_TABLE;

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf-8");
}

export const putItemHandler = async (event) => {
  if (event.httpMethod !== "POST") {
    throw new Error(
      `postMethod only accepts POST method, you tried: ${event.httpMethod} method.`
    );
  }

  console.info("received:", event);

  const body = JSON.parse(event.body);
  const id = body.id;
  const name = body.name;

  // Remove undefined values from the item
  const item = {
    id: id,
    name: name,
  };

  const params = {
    TableName: tableName,
    Item: item,
  };

  try {
    const data = await ddbDocClient.send(new PutCommand(params));
    console.log("Success - item added or updated", data);
  } catch (err) {
    console.log("Error", err.stack);
  }

  try {
    const s3Params = {
      Bucket: "ekjrfnkejrnfekrjnfekjrfnekjrfnerkjfnekjfenkj344k45345n34kj5",
      Key: "experience-manifest.json",
      ContentType: "application/json",
    };
    let existingManifestJson;
    try {
      const getObjectResponse = await s3Client.send(
        new GetObjectCommand({
          Bucket: "ekjrfnkejrnfekrjnfekjrfnekjrfnerkjfnekjfenkj344k45345n34kj5",
          Key: "experience-manifest.json",
        })
      );

      existingManifestJson = await streamToString(getObjectResponse.Body);
    } catch (err) {
      console.log("manifest is not available", err);
    }
    if (existingManifestJson) {
      const newManifestJson = [
        ...JSON.parse(existingManifestJson),
        {
          id: id,
          name: name,
        },
      ];
      const updatedManifestJson = JSON.stringify(newManifestJson);

      const updatedS3Params = {
        ...s3Params,
        Body: updatedManifestJson,
      };
      await s3Client.send(new PutObjectCommand(updatedS3Params));
      console.log("Success - manifest file updated in S3");
    } else {
      // Upload the manifest JSON to the S3 bucket
      const manifest = [
        {
          id: id,
          name: name,
        },
      ];

      // Convert the manifest object to JSON
      const manifestJson = JSON.stringify(manifest);

      // Create parameters for the S3 PutObjectCommand

      await s3Client.send(
        new PutObjectCommand({ ...s3Params, Body: manifestJson })
      );
    }
    console.log("Success - manifest file uploaded to S3");
  } catch (err) {
    console.log("Error uploading manifest file to S3", err.stack);
  }
  const response = {
    statusCode: 200,
    body: JSON.stringify(body),
  };

  console.info(
    `response from: ${event.path} statusCode: ${response.statusCode} body: ${response.body}`
  );
  return response;
};
