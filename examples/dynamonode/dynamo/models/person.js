'use strict';

const modella = require('modella');
const validators = require('modella-validators');
const shortid = require('shortid');

let modellaErrorsToString = require('../../helpers/modellaerrorstostring');

let PersonModella = modella('PersonModella');

PersonModella.use(validators);

let requiredStringKeys = ['location', 'id', 'email', 'firstName', 'lastName'];

requiredStringKeys.forEach(key => {
  PersonModella.attr(key, { required: true, type: 'string' });
});

PersonModella
  .attr('favoriteColors', { type: Array })
  .attr('address', { type: 'object' });

/**
 * Represents a row in the Dynamo person table
 * @param obj A plain javascript object to be added to the table.
 */
module.exports = function DynamoPerson(obj) {
  if (!obj.id) {
    let id = shortid.generate();
    obj.id = id;
  }

  if (!obj.createdAt) {
    obj.createdAt = Date.now();
  }

  obj.updatedAt = Date.now();

  let modella = new PersonModella(obj);
  if (!modella.isValid()) {
    throw new Error(modelHelper.modellaErrorsToString(modella.errors));
  }

  //Ensure lower case email
  obj.email = obj.email.toLowerCase();

  //Allow additional attributes
  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      this[key] = obj[key];
    }
  }

  //Do not allow additional attributes
  // let model = modella.toJSON();
  // for (let key in model) {
  //   if (model.hasOwnProperty(key)) {
  //     this[key] = model[key];
  //   }
  // }
};
