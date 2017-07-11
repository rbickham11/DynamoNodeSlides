'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const diff = require('deep-diff');
const moment = require('moment-timezone');
const Q = require('q');

const putObjectP = Q.nbind(s3.putObject, s3);
const deleteObjectP = Q.nbind(s3.deleteObject, s3);

const s3Bucket = 'dynamotalk-personupdates';

//Translates dynamodb item to plain js object
//(e.g. { name: { S: "Ryan" } } -> { name: "Ryan" })
const unmarshalItem = require('dynamodb-marshaler').unmarshalItem;

let writeItemSnapshots = (event, context, callback) => {
  //If there's no records for some reason, return
  if (!event.Records || event.Records.length < 1) {
    return callback(null);
  }

  //Retrieve table name from stream ARN
  let tableName = event.Records[0].eventSourceARN.split(':')[5].split('/')[1];
  let s3Writes = [];

  event.Records.forEach(record => {
    let oldItem = record.dynamodb.OldImage ? unmarshalItem(record.dynamodb.OldImage) : null;
    let newItem = record.dynamodb.NewImage ? unmarshalItem(record.dynamodb.NewImage) : null;

    let location = record.dynamodb.Keys.location.S;
    let itemId = record.dynamodb.Keys.id.S;

    let updatedDate = Date.now();

    let dateString = moment(updatedDate).tz('America/Detroit').format('M-D-YY_h:mm:ss:SSSa(z)');
    let diffFileKey = `${tableName}/${location}/${itemId}/history/${dateString}.json`;

    s3Writes.push(putObjectP({
      Bucket: s3Bucket,
      Key: diffFileKey,
      Body: JSON.stringify(new ChangeSnapshot(oldItem, newItem, updatedDate)),
      ContentType: 'application/json'
    }));
  });

  Q.all(s3Writes).then(() => {
    console.log(`Successfully processed ${event.Records.length} items`);
    callback(null, `Successfully processed ${event.Records.length} items`);
  }, err => {
    callback(err);
  });
};

module.exports.handler = writeItemSnapshots;

function ChangeSnapshot(oldItem, newItem, updatedDate) {
  this.updatedAt = updatedDate;
  this.oldItem = oldItem || {};
  this.newItem = newItem || {};

  if (oldItem && newItem) {
    this.diff = diff(this.oldItem, this.newItem);
  }
}
