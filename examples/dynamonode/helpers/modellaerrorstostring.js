'use strict';

module.exports = errors => {
  let errObj = {};

  errors.forEach(error => {
    errObj[error.attr] = error.message;
  });

  return JSON.stringify(errObj);
};
