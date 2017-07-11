'use strict';

const AWS = require('aws-sdk');
const https = require('https');
const Q = require('q');
const config = require('./config.json');

let dynamo = new AWS.DynamoDB({
  region: config.region,
  accessKeyId: config.accessKey,
  secretAccessKey: config.secretKey,
  httpOptions: {
    agent: new https.Agent({
      ciphers: 'ALL',
      secureProtocol: 'TLSv1_method'
    })
  }
});

let dynamoDocClient = new AWS.DynamoDB.DocumentClient({
  region: config.region,
  accessKeyId: config.accessKey,
  secretAccessKey: config.secretKey,
  httpOptions: {
    agent: new https.Agent({
      ciphers: 'ALL',
      secureProtocol: 'TLSv1_method'
    })
  }
});

let promiseMethods = {};

//Convert AWS callback based functions to promise based functions
for (let key in Object.getPrototypeOf(dynamo)) {
  if (typeof dynamo[key] === 'function') {
    promiseMethods[key] = Q.nbind(dynamo[key], dynamo);
  }
}

//Assign DynamoDocClient methods (http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html)
// to same object and prefer over raw DynamoDB methods
for (let key in Object.getPrototypeOf(dynamoDocClient)) {
  if (typeof dynamoDocClient[key] === 'function') {
    promiseMethods[key] = Q.nbind(dynamoDocClient[key], dynamoDocClient);
  }
}

module.exports = promiseMethods;
