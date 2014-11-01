'use strict';

var Bluebird = require('bluebird'),
    mkdirp = Bluebird.promisify(require('mkdirp')),
    fs = Bluebird.promisifyAll(require('fs')),
    request = require('request'),
    uuid = require('node-uuid'),
    unzip = require('unzip'),
    path = require('path');

function Downloader (config) {
  config = config || {};
  this.tmp = config.tmp || '/tmp';
}

Downloader.prototype.cities = function (num) {
  num = num || 1000;
  return this.fetch('dump/cities' + num + '.zip');
};

Downloader.prototype.country = function (country) {
  country = country || 'allCountries';
  return this.fetch('dump/' + country + '.zip');
};

Downloader.prototype.postcodes = function (country) {
  country = country || 'allCountries';
  return this.fetch('zip/' + country + '.zip');
};

Downloader.prototype.createTempFileName = function () {
  return path.join(this.tmp, uuid.v4());
};

Downloader.prototype.fetch = function (urlPath) {
  var self = this,
      filename = this.createTempFileName(),
      isZip = /\.zip$/.test(urlPath);
  return new Bluebird(function (resolve, reject) {
    request('http://download.geonames.org/export/' + urlPath)
    .pipe(fs.createWriteStream(filename))
    .on('finish', resolve)
    .on('error', reject);
  })
  .return(filename)
  .bind(this)
  .then(isZip ? this.unzip : function () { return filename; });
};

Downloader.prototype.unzip = function (filename) {
  var extracted = [],
      self = this;
  return new Bluebird(function (resolve, reject) {
    fs.createReadStream(filename)
    .pipe(unzip.Parse())
    .on('entry', function (entry) {
      if (entry.type === 'File' && !/read(.*)\.txt/.test(entry.path)) {
        var tmpFilename = self.createTempFileName();
        extracted.push(new Bluebird(function (resolve, reject) {
          entry
          .pipe(fs.createWriteStream(tmpFilename))
          .on('finish', resolve)
          .on('error', reject);
        }).return(tmpFilename));
      }
      else {
        entry.autodrain();
      }
    })
    .on('finish', resolve)
    .on('error', reject);
  })
  .then(function () {
    return Bluebird.all(extracted);
  });
};

module.exports = Downloader;