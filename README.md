# Geonames Importer

Imports geonames.org data into elasticsearch.

> Note: Work In Progress.

## Install
```sh
npm install --save geonames-importer
```

## Usage

```js
var Importer = require('geonames-importer');

var importer = new Importer({
  filename: 'cities1000.txt',
  index: 'myindex',
  transformers: [
    function (item) {
      return {
        id: item.id,
        name: item.name,
        geopoint: {
          lat: item.latitude,
          lon: item.longitude
        }
      };
    },
    function (item) {
      item.name = item.name.toUpperCase();
      return item;
    }
  ]
});

importer
.import()
.then(function () {
  console.log('finished');
})
.done();
```

## License

MIT