import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { MovieAward } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

type ResponseBody = {
  data: {
    movieAward: MovieAward;
  };
};
// Enable coercion so that the string 'true' is coerced to
// boolean true before validation is performed.
const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
  schema.definitions["MovieQueryParams"] || {}
);
const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    // Print Event
    console.log("Event: ", JSON.stringify(event));
    const parameters = event?.pathParameters;
    const movieId = parameters?.movieId
      ? parseInt(parameters.movieId)
      : undefined;
    const awardBody = parameters?.awardBody
    const numAwards = parameters?.numAwards
      ? parseInt(parameters.numAwards)
      : undefined;
    const awardDescription = parameters?.awardDescription
    const min = parameters?.min ? parseInt(parameters.min) : undefined;

    if (!movieId || !awardBody) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing movie Id or awardBody" }),
      };
    }


    let commandInput: QueryCommandInput = {
      TableName: process.env.TABLE_NAME,
      KeyConditionExpression: "movieId = :movieId and awardBody = :awardBody",
      ExpressionAttributeValues: {
        ":movieId": movieId,
        ":awardBody": awardBody,
      },
    };

    commandInput.ExpressionAttributeValues = commandInput.ExpressionAttributeValues || {};
    
    if (min !== undefined) {
      commandInput.FilterExpression = "numAwards >= :min";
      commandInput.ExpressionAttributeValues[":min"] = min;
    }

    const awardsCommandOutput = await ddbDocClient.send(
      new QueryCommand(commandInput)
    );

    const body = {
      data: awardsCommandOutput.Items,
    };


    // Return Response
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
