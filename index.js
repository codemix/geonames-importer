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


Importer.prototype.import = function () {
  var self = this;
  return new Bluebird(function (resolve, reject) {
    fs.createReadStream(self.filename)
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
    .on('end', resolve);
  });
};

Importer.prototype.addTransformer = function (transformer) {
  this.transformers.push(transformer);
  return this;
};

Importer.prototype.createCustomTransformStream = function () {
  var self = this;
  return through.obj(function (item, enc, next) {
    this.push(self.transformers.reduce(function (item, fn) {
      return fn(item);
    }, item));
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
