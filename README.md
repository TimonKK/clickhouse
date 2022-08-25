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

const clickhouse = new ClickHouse();
```
or with all options:

```javascript
const clickhouse = new ClickHouse({
	url: 'http://localhost',
	port: 8123,
	debug: false,
	basicAuth: null,
	isUseGzip: false,
	trimQuery: false,
	usePost: false,
	format: "json", // "json" || "csv" || "tsv"
	raw: false,
	config: {
		session_id                              : 'session_id if neeed',
		session_timeout                         : 60,
		output_format_json_quote_64bit_integers : 0,
		enable_http_compression                 : 0,
		database                                : 'my_database_name',
	},
	
	// This object merge with request params (see request lib docs)
	reqParams: {
		...
	}
});
```

or change 

	basicAuth: null
to

	basicAuth: {
		username: 'default',
		password: '',
	},


***
 
Exec query:
```javascript
const queries = [
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

for(const query of queries) {
	const r = await clickhouse.query(query).toPromise();

	console.log(query, r);
}
````

***

Exec by callback way:
```javascript
clickhouse.query(query).exec(function (err, rows) {
	...
});
````

***

Stream:
```javascript
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
	});
```

or **async** stream:
```javascript
// async iteration
for await (const row of clickhouse.query(sql).stream()) {
	console.log(row);
}
```

***

As promise:
```javascript
const rows = await clickhouse.query(query).toPromise();

// use query with external data
const rows = await clickhouse.query('SELECT * AS count FROM temp_table', {
	external: [
		{
			name: 'temp_table',
			data: e._.range(0, rowCount).map(i => `str${i}`)
		},
	]
}).toPromise();
```

***

Set session:
```javascript
clickhouse.sessionId = '...';
const r = await clickhouse.query(
	`CREATE TEMPORARY TABLE test_table
	(_id String, str String)
	ENGINE=Memory`
).toPromise();
````

In case your application requires specific sessions to manage specific data then you can send `session_id` with each query.

```javascript
let mySessionId = 'some_randome_string';
const r = await clickhouse.query(
	`CREATE TEMPORARY TABLE test_table
	(_id String, str String)
	ENGINE=Memory`, {}, {sessionId: mySessionId}
).toPromise();
```

Insert stream:
```javascript
const ws = clickhouse.insert('INSERT INTO session_temp').stream();
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
const result = await ws.exec();
```

***

Pipe readable stream to writable stream (across transform):
```javascript
const rs = clickhouse.query(query).stream();

const tf = new stream.Transform({
	objectMode : true,
	transform  : function (chunk, enc, cb) {
		cb(null, JSON.stringify(chunk) + '\n');
	}
});

clickhouse.sessionId = Date.now();
const ws = clickhouse.insert('INSERT INTO session_temp2').stream();

const result = await rs.pipe(tf).pipe(ws).exec();
```
***

insert array of objects:
```javascript

/*
	CREATE TABLE IF NOT EXISTS test_array (
				date Date,
				str String,
				arr Array(String),
				arr2 Array(Date),
				arr3 Array(UInt8),
				id1 UUID
			) ENGINE=MergeTree(date, date, 8192)
*/
		const rows = [
			{
				date: '2018-01-01',
				str: 'Something1...',
				arr: [],
				arr2: ['1985-01-02', '1985-01-03'],
				arr3: [1,2,3,4,5],
				id1: '102a05cb-8aaf-4f11-a442-20c3558e4384'
			},
			
			{
				date: '2018-02-01',
				str: 'Something2...',
				arr: ['5670000000', 'Something3...'],
				arr2: ['1985-02-02'],
				arr3: [],
				id1: 'c2103985-9a1e-4f4a-b288-b292b5209de1'
			}
		];
		
		await clickhouse.insert(
			`insert into test_array 
			(date, str, arr, arr2, 
			 arr3, id1)`,
			rows
		).toPromise();
```
***

Parameterized Values:
```javascript
const rows = await clickhouse.query(
	'SELECT * AS count FROM temp_table WHERE version = {ver:UInt16}',
	{
		params: {
			ver: 1
		},
	}
).toPromise();
```
For more information on encoding in the query, see [this section](https://clickhouse.com/docs/en/interfaces/http/#cli-queries-with-parameters) of the ClickHouse documentation.

***

**Run Tests**:

```
npm install
npm run test
# or
# node_modules/.bin/mocha --timeout 60000 --slow 5000 -f "SPECIFIC TEST NAME"
```

***

**Changelogs**:
* 2020-08-26 (v2.6.0)
	- A lot of PRs from community
* 2020-11-02 (v2.4.1)
	- Merge list of PR
	- fix test with Base Auth check
* 2020-11-02 (v2.2.0) ___Backward Incompatible Change___
    - port from url more important than port from config
* 2020-04-17 (v2.1.0)
    - Fix query with totals. For json formats work perfect, but for another - doesn't
* 2019-02-13
	- Add compatibility with user and username options 
* 2019-02-07
	- Add TLS/SSL Protocol support
	- Add async iteration over SELECT
	
	
	
**Links**
* request lib doc https://github.com/request/request#requestoptions-callback
