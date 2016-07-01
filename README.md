# clickhouse
NodeJS client for [ClickHouse](https://clickhouse.yandex/).
Send query over HTTP interface.

Example:

```javascript
var async = require('async');
 
var query = 'SELECT FlightDate, DestCityName, AirlineID, DestStateFips FROM ontime LIMIT 10';
var ch = new ClickHouse({
	url   : 'http://localhost',
	port  : 8123,
	debug : false
});

async.parallel(
	[
		function (cb) {

			// single query
			ch.query(query, function (err, rows) {
				console.log('single query result', err, rows);
				
				cb(err);
			});
		},
		
		function (cb) {
			var error = null;
			
			// query with data streaming
			ch.query(query)
				.on('data', function (data) {
					console.log('data', data);
				})
				.on('error', function (err) {
					error = err;
				})
				.on('end', function () {
					cb(error);
				});
		},

		function(cb) {

			// insert rows
			ch.insertMany(
				'sometable',
				[
					// row 1
					[
						'2016-07-02',
						'1',
						12
					],

					// row 2
					[
						'2016-07-03',
						'2',
						30
					]
				],
				function(err, result) {
					if (err)  return cb(err);

					console.log('insert result', result);
				}
			);
		}
	],
	function () {
		process.exit();
	}
);

```