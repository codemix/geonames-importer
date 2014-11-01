'use strict';

var Bluebird = require('bluebird'),
    Stream = require('stream'),
    csv = require('csv'),
    fs = Bluebird.promisifyAll(require('fs')),
    through = require('through2'),
    elasticsearch = require('elasticsearch'),
    elasticsearchStreams = require('elasticsearch-streams'),
    WritableBulk = elasticsearchStreams.WritableBulk,
    TransformToBulk = elasticsearchStreams.TransformToBulk;


function Importer (config) {
  config = config || {};
  this.filename = config.filename || "geonames.txt";
  this.client = config.client || new elasticsearch.Client(config.elasticsearch || {
    host: 'localhost:9200'
  });
  this.idField = config.idField || 'id';
  this.index = config.index || 'geoname';
  this.type = config.type || 'geoname';
  this.transformers = config.transformers || [];
}

module.exports = Importer;


Importer.prototype.import = function (filename) {
  var self = this;
  return Bluebird.cast(filename || self.filename)
  .then(function (filename)) {
    return new Bluebird(function (resolve, reject) {
      fs.createReadStream(filename)
      .pipe(csv.parse({
        delimiter: "\t",
        quote: false,
        columns : [
          'id',
          'name',
          'asciiName',
          'alternateNames',
          'latitude',
          'longitude',
          'featureClass',
          'featureCode',
          'countryCode',
          'cc2',
          'admin1Code',
          'admin2Code',
          'admin3Code',
          'admin4Code',
          'population',
          'elevation',
          'dem',
          'timezone',
          'modificationDate'
        ]
      }))
      .pipe(self.createCustomTransformStream())
      .pipe(self.createBulkTransformStream())
      .pipe(self.createBulkImportStream())
      .on('error', reject)
      .on('finish', resolve);
    })
  });
};

Importer.prototype.postcodes = function (filename) {
  var self = this;
  return Bluebird.cast(filename || self.filename)
  .then(function (filename)) {
    return new Bluebird(function (resolve, reject) {
      fs.createReadStream(filename)
      .pipe(csv.parse({
        delimiter: "\t",
        quote: false,
        columns : [
          'countryCode',
          'postalCode',
          'placeName',
          'admin1Name',
          'admin1Code',
          'admin2Name',
          'admin2Code',
          'admin3Name',
          'admin3Code',
          'latitude',
          'longitude',
          'accuracy'
        ]
      }))
      .pipe(self.createCustomTransformStream())
      .pipe(self.createBulkTransformStream())
      .pipe(self.createBulkImportStream())
      .on('error', reject)
      .on('finish', resolve);
    });
  });
};


Importer.prototype.addTransformer = function (transformer) {
  this.transformers.push(transformer);
  return this;
};

Importer.prototype.createCustomTransformStream = function () {
  var self = this;
  return through.obj(function (item, enc, next) {
    var result = self.transformers.reduce(function (item, fn) {
      if (!item) {
        return item;
      }
      else {
        return fn(item);
      }
    }, item);
    if (result) {
      this.push(result);
    }
    next();
  });
};

Importer.prototype.createBulkTransformStream = function () {
  var self = this;
  return new TransformToBulk(function (doc) {
    return {
      _index: self.index,
      _type: self.type,
      _id: doc[self.idField]
    };
  });
};

Importer.prototype.createBulkImportStream = function () {
  var self = this;
  return new WritableBulk(function (commands, callback) {
    self.client.bulk({
      index: self.index,
      body: commands
    }, callback);
  }, 32);
};
