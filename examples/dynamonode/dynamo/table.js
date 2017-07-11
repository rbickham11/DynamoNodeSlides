'use strict';

const R = require('ramda');
const Q = require('q');

const dynamo = require('./driver');

class DynamoTable {
  /**
   * Represents a dynamo table
   * @constructor DynamoTable
   * @param {Object} tableDesc A Dynamo table description retrieved from a [describeTable operation]{@link http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#describeTable-property}
   * @param {function} objectConstructor A constructor that all objects created should be passed through to ensure validity
   * @params {Object} [options] An object containing functionality options
   * @param {boolean} [options.constructOnGet] If set to 'true', objects retrieved from Dynamo will also be passed through the given objectConstructor.
   * This can be useful for setting default values and/or ensuring table values are valid.
   * @return {DynamoTable}
   */
  constructor(tableDesc, objectConstructor, options) {
    if (!tableDesc) {
      throw new Error('Table description object is required');
    }

    /**
     * The associated table's name
     * @name DynamoTable#tableName
     * @type string
     */
    this.tableName = tableDesc.TableName;

    /**
     * The attribute name of the table's partition key
     * @name DynamoTable#partitionKey
     * @type string
     */
    this.partitionKey = R.find(k => k.KeyType === 'HASH', tableDesc.KeySchema).AttributeName;

    let sortKeySchema = R.find(k => k.KeyType === 'RANGE', tableDesc.KeySchema);
    /**
     * The attribute name of the table's sort key, if one exists
     * @name DynamoTable#sortKey
     * @type string | null
     * @default null
     */
    this.sortKey = sortKeySchema ? sortKeySchema.AttributeName : null;

    /**
     * The total number of items in the table
     * @name DynamoTable#itemCount
     * @type number
     */
    this.itemCount = tableDesc.ItemCount;

    /**
     * The tableDescription response that was used to genrate the model
     * @name DynamoTable#tableDesc
     * @type string
     */
    this.tableDesc = tableDesc;

    /**
     * A constructor that all objects created/retrieved should be passed through to ensure validity
     * @name DynamoTable#objectConstructor
     * @type function
     */
    this.objectConstructor = objectConstructor;

    /**
     * An object containing functionality options
     * @name DynamoTable#options
     * @type object
     */
    this.options = options || {};
  }

  /**
   * Deletes an item by its key
   * @param {string} partitionKey The partition key value
   * @param {string} [sortKey] The sort key value (if one exists)
   * @return {Promise} A promise that resolves when the request is successful
   */
  deleteItem(partitionKey, sortKey) {
    return dynamo.delete(this.buildKeyParameters(partitionKey, sortKey));
  }

  /**
   * Retrieves an item by its key
   * @param {string} partitionKey The partition key value
   * @param {string} [sortKey] The sort key value (if one exists)
   * @return {Promise} A promise that resolves with the requested item
   */
  getItem(partitionKey, sortKey) {
    return dynamo.get(this.buildKeyParameters(partitionKey, sortKey)).then(result => {
      if (!result.Item) {
        return undefined;
      }

      return this.mapItemResult(result.Item);
    });
  }

  /**
   * Adds an item to the model's table
   * @param {Object} item The item to be added to the table. The table's associated keys are required as properties,
   * all other properties are optional.
   * @return {Promise} A promise that resolves when the operation is successful
   */
  putItem(item) {
    if (this.objectConstructor) {
      try {
        item = newObject(this.objectConstructor, [item]);
      } catch (err) {
        return Q.reject(err);
      }
    }

    let plainItem = toPlainObject(item);
    deleteEmptyStrings(plainItem);

    let params = {
      Item: plainItem,
      TableName: this.tableName
    };


    return dynamo.put(params).then(() => item);
  }

  /**
   * Performs a simple query on the underlying table, returning all matching items. This will automatically detect
   * indexes, keys, etc from the given parameters.
   * @param {Object} [keyValueParameters] An object containing key value pairs to query an exact match for.
   * @param {Object} [containsQuery] An object containing key value pairs to perform a "CONTAINS" query on.
   * @param {Object} [betweenQuery] An object containing key value pairs for a between query (ex: { num: [1, 10] })
   * @return {Promise} A promise that resolves with the objects resulting from the query
   */
  query(keyValueParameters, containsQuery, betweenQuery) {
    keyValueParameters = keyValueParameters || {};

    return new Promise((resolve, reject) => {
      let queryParams = {
        TableName: this.tableName
      };

      //The key schema to use for the query
      let matchingKeySchema;

      //Determines if a given KeySchema (from a table or index) matches two of the keyValue parameters exactly
      let fitsParamsExactly = keySchema => {
        for (let i = 0; i < keySchema.length; i++) {
          let key = keySchema[i];
          if (!keyValueParameters[key.AttributeName] || keyValueParameters[key.AttributeName].constructor === Array) {
            return false;
          }
        }
        return true;
      };

      let partitionKeyValue = keyValueParameters[this.partitionKey];

      if (fitsParamsExactly(this.tableDesc.KeySchema)) {
        matchingKeySchema = this.tableDesc.KeySchema;
      } else {
        let lsiFound = false;

        if (partitionKeyValue) {
          matchingKeySchema = this.tableDesc.KeySchema;

          if (this.tableDesc.LocalSecondaryIndexes) {
            //Query on table is still valid with only partition key, but indexes may be faster
            //Check local secondary indexes
            for (let i = 0; i < this.tableDesc.LocalSecondaryIndexes.length; i++) {
              let index = this.tableDesc.LocalSecondaryIndexes[i];
              if (fitsParamsExactly(index.KeySchema)) {
                queryParams.IndexName = index.IndexName;
                matchingKeySchema = index.KeySchema;

                lsiFound = true;

                break;
              }
            }
          }
        }

        if (!lsiFound) {
          //Without a partition key value provided, a global secondary index must be found to perform a query
          //Check global secondary indexes
          if (this.tableDesc.GlobalSecondaryIndexes) {
            for (let i = 0; i < this.tableDesc.GlobalSecondaryIndexes.length; i++) {
              let index = this.tableDesc.GlobalSecondaryIndexes[i];
              if (fitsParamsExactly(index.KeySchema)) {
                queryParams.IndexName = index.IndexName;
                matchingKeySchema = index.KeySchema;
                break;
              }
            }
            if (!matchingKeySchema) {
              //If no exact matches were found, get the first with at least a matching partition key
              for (let i = 0; i < this.tableDesc.GlobalSecondaryIndexes.length; i++) {
                let index = this.tableDesc.GlobalSecondaryIndexes[i];
                let pk = R.find(key => key.KeyType === 'HASH', index.KeySchema);
                if (keyValueParameters[pk.AttributeName]) {
                  matchingKeySchema = index.KeySchema;
                  queryParams.IndexName = index.IndexName;
                  break;
                }
              }
            }
          }

          if (!matchingKeySchema) {
            //There are no valid keys for querying, so do a scan
            resolve(this.scan(keyValueParameters, containsQuery, betweenQuery));

            return;
          }
        }
      }


      queryParams.ExpressionAttributeNames = {};
      queryParams.ExpressionAttributeValues = {};

      queryParams.KeyConditionExpression = buildKeyConditionExpression(
        matchingKeySchema,
        keyValueParameters,
        queryParams.ExpressionAttributeNames,
        queryParams.ExpressionAttributeValues
      );

      queryParams.FilterExpression = buildFilterExpression(
        matchingKeySchema,
        keyValueParameters,
        containsQuery,
        betweenQuery,
        queryParams.ExpressionAttributeNames,
        queryParams.ExpressionAttributeValues
      );

      dynamo.query(queryParams).then(result => {
        let mapItemResult = this.mapItemResult.bind(this);

        let getAllPages = result => {
          let items = result.Items;

          return new Promise((resolve, reject) => {
            let getPage = startKey => {
              if (startKey) {
                queryParams.ExclusiveStartKey = startKey;
                dynamo.query(queryParams).then(pageResult => {
                  items = R.concat(items, pageResult.Items);
                  getPage(pageResult.LastEvaluatedKey);
                }, err => {
                  reject(err);
                });
              } else {
                resolve(items);
              }
            };

            getPage(result.LastEvaluatedKey);
          });
        };

        getAllPages(result).then(allItems => {
          resolve(R.map(mapItemResult, allItems));
        });
      }, err => {
        reject(err);
      });
    });
  }

  /**
   * Performs a custom query using a raw Dynamo query object. This allows for more flexibility, but is less convenient
   * @param queryParams A query object matching the
   * [AWS SDK specification]{@link http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#query-property}
   * @return {Promise} A promise that resolves with the objects resulting from the query
   */
  rawQuery(queryParams) {
    queryParams.TableName = this.tableName;

    return dynamo.query(queryParams).then(results => {
      let mapItemResult = this.mapItemResult.bind(this);
      return R.map(mapItemResult, results.Items);
    });
  }

  /**
   * Returns all items in the table with an optional filter
   * @param {Object} [keyValueParameters] An optional set of key value pairs to filter by
   * @param {Object} [containsQuery] An object containing key value pairs to perform a "CONTAINS" query on.
   * @param {Object} [betweenQuery] An object containing key value pairs for a between query (ex: { num: [1, 10] })
   * @return {Promise} A promise that resolves with the objects resulting from the scan
   */
  scan(keyValueParameters, containsQuery, betweenQuery) {
    keyValueParameters = keyValueParameters || {};

    let scanParams = {
      TableName: this.tableName
    };

      //If performing an IN query with the sortKey, replace with the filterable key attribute
    if (this.sortKey && keyValueParameters[this.sortKey] && keyValueParameters[this.sortKey].constructor === Array) {
      keyValueParameters[`${this.sortKey}_filterable`] = keyValueParameters[this.sortKey];
      delete keyValueParameters[this.sortKey];
    }

    if (Object.keys(keyValueParameters).length > 0) {
      scanParams.ExpressionAttributeNames = {};
      scanParams.ExpressionAttributeValues = {};

      scanParams.FilterExpression = buildFilterExpression(
        this.tableDesc.KeySchema,
        keyValueParameters,
        containsQuery,
        betweenQuery,
        scanParams.ExpressionAttributeNames,
        scanParams.ExpressionAttributeValues
      );
    }

    return dynamo.scan(scanParams).then(result => {
      let mapItemResult = this.mapItemResult.bind(this);

      let getAllPages = result => {
        let items = result.Items;

        return new Promise((resolve, reject) => {
          let getPage = startKey => {
            if (startKey) {
              scanParams.ExclusiveStartKey = startKey;
              dynamo.scan(scanParams).then(pageResult => {
                items = R.concat(items, pageResult.Items);
                getPage(pageResult.LastEvaluatedKey);
              }, err => {
                reject(err);
              });
            } else {
              resolve(items);
            }
          };

          getPage(result.LastEvaluatedKey);
        });
      };

      return getAllPages(result).then(allItems => {
        return R.map(mapItemResult, allItems);
      });
    });

  }

  /**
   * Maps an item returned from a query/scan into a more readable JSON form
   * and removes unnecessary properties.
   * @param {Object} item An item returned from a query/scan
   */
  mapItemResult(item) {
    //Turn set types into arrays
    for (let key in item) {
      if (item[key] && item.hasOwnProperty(key) && item[key].constructor.name === 'Set') {
        item[key] = item[key].values;
      }
    }

    if (this.options.constructOnGet && this.objectConstructor) {
      item = newObject(this.objectConstructor, [item]);
    }

    return item;
  }

  /**
   * Builds a key based query for a getItem or deleteItem operation.
   * @param {string} partitionKey The partition key
   * @param {string} [sortKey] The sort key (if one exists)
   * @return {Object} The resulting parameter object
   */
  buildKeyParameters(partitionKey, sortKey) {
    let params = {
      Key: {},
      TableName: this.tableName
    };

    params.Key[this.partitionKey] = partitionKey;
    params.Key[this.sortKey] = sortKey;

    return params;
  }
}

module.exports = DynamoTable;


/**
 * Dynamically calls 'new' on the provided constructor with the provided arguments
 * @param {function} constructor The constructor to be applied
 * @param {Array} args The arguments to pass
 * @returns {Object} The resulting attribute pair
 */
function newObject(constructor, args) {
  args.unshift(constructor);
  return new (constructor.bind.apply(constructor, args))();
}

/**
 * Converts an object of any type to a plain object
 * @param {*} obj The object to be converted
 * @returns {object} The resulting object
 */
function toPlainObject(obj) {
  let plainObj = {};

  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      plainObj[key] = obj[key];
    }
  }

  return plainObj;
}

/**
 * Builds a KeyConditionExpression from the given schema and key value pairs. Also appends the key values
 * to the given ExpressionAttributeValues object
 * @param keySchema An object representing the KeySchema for the query
 * @param keyValuePairs An object that contains values for the keys to search for
 * @param expressionAttributeNames An object that will have necessary attribute names mapped
 * @param expressionAttributeValues An object that will have necessary attribute values mapped
 */
function buildKeyConditionExpression(keySchema, keyValuePairs, expressionAttributeNames, expressionAttributeValues) {
  let expression = '';

  let partitionKey = R.find(key => key.KeyType === 'HASH', keySchema);
  if (!partitionKey) {
    throw new Error('Invalid keySchema');
  }

  let addKeyToExpression = key => {
    let keyPlaceholder = `#${key.AttributeName}`;
    let valuePlaceholder = `:${key.AttributeName}`;

    expression += `${keyPlaceholder} = ${valuePlaceholder}`;

    expressionAttributeNames[keyPlaceholder] = key.AttributeName;
    expressionAttributeValues[valuePlaceholder] = keyValuePairs[key.AttributeName];
  };

  let pkValue = keyValuePairs[partitionKey.AttributeName];
  if (!pkValue) {
    throw new Error(`Partition key ${partitionKey.AttributeName} is required`);
  }

  addKeyToExpression(partitionKey);

  let sortKey = R.find(key => key.KeyType === 'RANGE', keySchema);
  let sortKeyValue = sortKey ? keyValuePairs[sortKey.AttributeName] : undefined;

  if (sortKey && sortKeyValue) {
    expression += ' AND ';
    addKeyToExpression(sortKey);
  }

  return expression;
}

/**
 * Builds a FilterExpression based on the given schema and key value pairs. Also appends the key values
 * to the given ExpressionAttributeValues object
 * @param keySchema An object representing the KeySchema for the query.
 * @param keyValuePairs An object that contains values for the keys to search for
 * @param {Object} [containsQuery] An object containing key value pairs to perform a "CONTAINS" query on.
 * @param {Object} [betweenQuery] An object containing key value pairs for a between query (ex: { num: [1, 10] })
 * @param expressionAttributeNames An object that will have necessary attribute names mapped
 * @param expressionAttributeValues An object that will have necessary attribute values mapped
 */
function buildFilterExpression(keySchema, keyValuePairs, containsQuery, betweenQuery, expressionAttributeNames,
expressionAttributeValues) {
  let searchTerms = R.clone(keyValuePairs);

  //Remove key parameters
  keySchema.forEach(key => {
    delete searchTerms[key.AttributeName];
  });

  let expression = '';

  let buildKeyPlaceholder = key => {
    let keyParts = key.split('.');
    let dotlessKey = keyParts.join('_');

    let partPlaceholders = [];

    keyParts.forEach((part, index) => {
      let partPlaceholder = `#${dotlessKey}${index}`;

      expressionAttributeNames[partPlaceholder] = part;

      partPlaceholders.push(partPlaceholder);
    });

    return partPlaceholders.join('.');
  };

  let addSearchTermToExpression = (key, value) => {
    let valuePlaceholder = `:${key}`.split('.').join('_');
    expressionAttributeValues[valuePlaceholder] = value;

    expression += `${buildKeyPlaceholder(key)} = ${valuePlaceholder}`;
  };

  let addInQueryToExpression = (key, values) => {
    let keyPlaceholder = buildKeyPlaceholder(key);

    let valuePlaceholders = [];
    values.forEach((value, index) => {
      let placeholder = `:${key}${index}`.split('.').join('_');
      valuePlaceholders.push(placeholder);

      expressionAttributeValues[placeholder] = value;
    });

    expression += `${keyPlaceholder} IN (${valuePlaceholders.join(',')})`;
  };

  let addContainsQueryToExpression = (key, value) => {
    let keyPlaceholder = buildKeyPlaceholder(key);
    let valuePlaceholder = `:${key}`.split('.').join('_');

    expressionAttributeValues[valuePlaceholder] = value;

    expression += `contains(${keyPlaceholder}, ${valuePlaceholder})`;
  };

  let addBetweenQueryToExpression = (key, values) => {
    let keyPlaceholder = buildKeyPlaceholder(key);

    let p1 = `:${key}1`.split('.').join('_');

    if (!values[0]) {
      expression += `${keyPlaceholder} <= ${p1}`;
      expressionAttributeValues[p1] = values[1];
    } else if (!values[1]) {
      expression += `${keyPlaceholder} >= ${p1}`;
      expressionAttributeValues[p1] = values[0];
    } else {
      let p2 = `:${key}2`.split('.').join('_');

      expression += `${keyPlaceholder} BETWEEN ${p1} AND ${p2}`;
      expressionAttributeValues[p1] = values[0];
      expressionAttributeValues[p2] = values[1];
    }
  };

  for (let key in searchTerms) {
    if (expression.length > 0) {
      expression += ' AND ';
    }
    if (searchTerms[key].constructor === Array) {
      addInQueryToExpression(key, searchTerms[key]);
    } else {
      addSearchTermToExpression(key, searchTerms[key]);
    }
  }

  if (containsQuery) {
    for (let key in containsQuery) {
      if (expression.length > 0) {
        expression += ' AND ';
      }
      addContainsQueryToExpression(key, containsQuery[key]);
    }
  }

  if (betweenQuery) {
    for (let key in betweenQuery) {
      if (expression.length > 0) {
        expression += ' AND ';
      }
      addBetweenQueryToExpression(key, betweenQuery[key]);
    }
  }

  return expression.length > 0 ? expression : undefined;
}

function deleteEmptyStrings(obj) {
  if (typeof obj !== 'object') {
    throw new Error('Invalid object');
  }

  for (let i in obj) {
    if (obj[i] === '') {
      delete obj[i];
    } else if (typeof obj[i] === 'object') {
      deleteEmptyStrings(obj[i]);
    }
  }
};
