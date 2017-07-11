'use strict';

/**
 * This serves as a wrapper for the app wide DynamoTableCollection instance. Any application that requires a
 * global instance should require this and set the 'appTables' property equal to a DynamoTableCollection
 * instance to be used app wide.
 */
module.exports = {
  appTables: null
}
