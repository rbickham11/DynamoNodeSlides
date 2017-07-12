'use strict';

const R = require('ramda');
const Q = require('q');

const dynamo = require('./driver');
const DynamoTable = require('./table');
const DynamoModels = require('./models');

class DynamoTableCollection {
  /**
   * Represents a collection of dynamo tables that are retrieved based on optional parameters and can be used in
   * an application
   * @constructor DynamoTableCollection
   * @param {string} [tablePrefix] A string prefix on which to filter the applied tables by
   * See {@link DynamoTable} for further detail
   * @return {DynamoTableCollection}
   */
  constructor(tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  /**
   * Finds tables in the data store, optionally with the provided prefix, and creates the necessary table objects
   * @return {Promise} A promise that resolves when the models have been initiated
   */
  initialize() {
    return new Promise((resolve, reject) => {
      dynamo.listTables({}).then(result => {
        let tableNames = result.TableNames;
        let tablesToRetrieve;

        if (this.tablePrefix) {
          tablesToRetrieve = R.filter(name => name.startsWith(this.tablePrefix), tableNames);
        } else {
          tablesToRetrieve = tableNames;
        }

        let describePromises = R.map(name => dynamo.describeTable({ TableName: name }), tablesToRetrieve);

        Q.all(describePromises).then(results => {
          /**
           * The generated model associated with the retrieved person table. If no person table was
           * retrieved, this property will be undefined
           * @name DynamoTableCollection#person
           * @type DynamoTable
           */
          this.people = buildTableFromName(
            results,
            this.tablePrefix,
            'people',
            DynamoModels.DynamoPerson
          );

          resolve();
        }, err => {
          reject(err);
        });
      }, err => {
        reject(err);
      });
    });
  }
}

module.exports = DynamoTableCollection;

/**
 * Searches through the given tableDescriptions collection and returns a DynamoTable instance
 * if a description exists for the given tableName.
 * @param tableDescriptions A retrieved collection of tableDescriptions
 * @param [tablePrefix] A table prefix to prepend to the searched tableName
 * @param tableName The name of the table to search for
 * @param {function} objectConstructor A constructor to pass through to the table object
 * @param {function} objectConstructor Additional options to pass through to the table object
 */
function buildTableFromName(tableDescriptions, tablePrefix, tableName, objectConstructor, modelOptions) {
  let tables = R.pluck('Table', tableDescriptions);
  let searchTerm = tablePrefix ? `${tablePrefix}${tableName}` : tableName;
  let foundTable = R.find(R.propEq('TableName', searchTerm), tables);

  if (!foundTable) {
    return undefined;
  }

  return new DynamoTable(foundTable, objectConstructor, modelOptions);
}
