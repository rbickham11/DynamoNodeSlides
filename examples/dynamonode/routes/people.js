'use strict';

const R = require('ramda');
const Q = require('q');
const express = require('express');
const router = express.Router();

const dynamo = require('../dynamo/provider').appTables;

router.get('/', (req, res, next) => {
  return dynamo.people.query(req.query).then(people => {
    res.status(200).json(people);
  }, err => {
    res.status(500).json(err);
  });
});

router.get('/:location/:id', (req, res, next) => {
  return dynamo.people.getItem(req.params.location, req.params.id).then(person => {
    if (!person) {
      return res.status(404).json({ message: `Person with key '${req.params.location}' , '${req.params.id}' does not exist`});
    }

    res.status(200).json(person);
  }, err => {
    res.status(500).json(err);
  });
});

module.exports = router;
