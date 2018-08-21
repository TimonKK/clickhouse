# clickhouse
NodeJS client for [ClickHouse](https://clickhouse.yandex/).
Send query over HTTP interface.

Install:

```bash
npm i clickhouse
```

Example:

```javascript
const { ClickHouse } = require('clickhouse');

// all options
const clickhouse = new ClickHouse();
```
or

```javascript
// all options
const clickhouse = new ClickHouse({
	url: 'http://localhost',
	port: 8123,
	debug: false,
	user: 'default',
	password: '',
	basicAuth: null,
	isUseGzip: false,
	config: {
		session_timeout                         : 60,
		output_format_json_quote_64bit_integers : 0,
		enable_http_compression                 : 0
	}
});
```

```javascript
// exec query
let queries = [
	'DROP TABLE IF EXISTS session_temp',

	`CREATE TABLE session_temp (
		date Date,
		time DateTime,
		mark String,
		ips Array(UInt32),
		queries Nested (
			act String,
			id UInt32
		)
	)
	ENGINE=MergeTree(date, (mark, time), 8192)`,

	'OPTIMIZE TABLE ukit.loadstat PARTITION 201807 FINAL'
];

for(let query of queries) {
	let r = await clickhouse.query(query).toPromise();

	console.log(query, r);
}


// exec by callback way
clickhouse.query(query).exec(function (err, rows) {
	...
});

// stream
clickhouse.query(`SELECT number FROM system.numbers LIMIT 10`).stream()
	.on('data', function() {
		const stream = this;

		stream.pause();

		setTimeout(() => {
			stream.resume();
		}, 1000);
	})
	.on('error', err => {
		...
	})
	.on('end', () => {
		...
	})


// as promise
let rows = await clickhouse.query(query).toPromise();

// use query with external data
let rows = await clickhouse.query('SELECT * AS count FROM temp_table', {
	external: [
		{
			name: 'temp_table',
			data: e._.range(0, rowCount).map(i => `str${i}`)
		},
	]
}).toPromise();


// set session
clickhouse.sessionId = '...';
let r = await clickhouse.query(
	`CREATE TEMPORARY TABLE test_table
	(_id String, str String)
	ENGINE=Memory`
).toPromise();


// insert stream
let ws = clickhouse.insert('INSERT INTO session_temp').stream();
for(let i = 0; i <= 1000; i++) {
	await ws.writeRow(
		[
			e._.range(0, 50).map(
				j => `${i}:${i * 2}:${j}`
			).join('-')
		]
	);
}

//wait stream finish
let result = await s.exec();


// pipe readable stream to writable stream (across transform)
let rs = clickhouse.query(query).stream();

let tf = new stream.Transform({
	objectMode : true,
	transform  : function (chunk, enc, cb) {
		cb(null, JSON.stringify(chunk) + '\n');
	}
});

clickhouse.sessionId = Date.now();
let ws = clickhouse.insert('INSERT INTO session_temp2').stream();

let result = await rs.pipe(tf).pipe(ws).exec();
```