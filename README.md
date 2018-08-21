# clickhouse
NodeJS client for [ClickHouse](https://clickhouse.yandex/).
Send query over HTTP interface.

Install:

```bash
npm i clickhouse
```

Example:

```javascript
var async = require('async');
var ClickHouse = require('clickhouse');
 
let clickhouse = e.h.clickhouseV2;
		
e.async.series(
	[
		async function (cb) {
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

			cb();
		},

		function (cb) {
			// Нельзя записать много (десятки милионов), потому что
			// https://github.com/request/request/issues/2826
			let rowCount = e._.random(50 * 1024, 1024 * 1024);

			let queries = [
				`SELECT number, toString(number * 2), toDate(number + 1) FROM system.numbers LIMIT ${rowCount}`,
			];

			e.async.mapSeries(
				queries, function (query, cb) {
					e.async.series(
						[
							function(cb) {
								clickhouse.query(query).exec(cb.ok(function (rows) {
									if (rows.length !== rowCount) throw new Error(`1: ${rows.length} !== ${rowCount}`);

									cb();
								}));
							},

							function(cb) {
								let error = null;
								let c = 0;

								clickhouse.query(query).stream()
									.on('data', function() {
										++c;
									})
									.on('error', err => {
										console.log('2 error', err);
										error = err;
									})
									.on('end', () => {
										if (c !== rowCount) throw new Error('2');

										cb(error);
									})
							},


							function(cb) {
								let error = null;
								let c = 0;
								let count = 10;

								clickhouse.query(`SELECT number FROM system.numbers LIMIT ${count}`).stream()
									.on('data', function() {
										const stream = this;

										++c;

										stream.pause();

										setTimeout(() => {
											console.log('c', c);


											stream.resume();
										}, 1000);
									})
									.on('error', err => {
										console.log('22 error', err);
										error = err;
									})
									.on('end', () => {
										if (c !== count) throw new Error(`22 error: ${c} !== ${count}`);

										cb(error);
									})
							},

							// Waiting for Node.js 10
							// async function (cb) {
							// 	for await (const row of await clickhouse.query(query).stream()) {
							// 		console.log('row', row)
							// 	}
							//	
							// 	cb();
							// },

							function(cb) {
								let error = null;
								let c = 0;

								clickhouse.query(query).stream()
									.on('data', function() {
										++c;
									})
									.on('error', err => {
										console.log('2 error', err);
										error = err;
									})
									.on('end', () => {
										if (c !== rowCount) throw new Error('2');

										cb(error);
									})
							},

							async function (cb) {
								let rows = await clickhouse.query(query).toPromise();

								if (rows.length !== rowCount) throw new Error(`3: ${rows.length} !== ${rowCount}`);

								cb();
							},

							// Тест для внешних данных для запроса
							async function (cb) {
								let rows = await clickhouse.query('SELECT count(*) AS count FROM temp_table', {
									external: [
										{
											name: 'temp_table',
											data: e._.range(0, rowCount).map(i => `str${i}`)
										},
									]
								}).toPromise();

								if (rows[0].count !== rowCount) throw new Error(`4: ${rows[0].count} !== ${rowCount}`);

								cb();
							},

							// Тест сессии
							async function (cb) {
								clickhouse.sessionId = Date.now();
								let r = await clickhouse.query(
									`CREATE TEMPORARY TABLE test_table
									(_id String, str String)
									ENGINE=Memory`
								).toPromise();

								clickhouse.sessionId = Date.now();
								let r2 = await clickhouse.query(
									`CREATE TEMPORARY TABLE test_table
									(_id String, str String)
									ENGINE=Memory`
								).toPromise();

								cb();
							},

							async function (cb) {
								let r = await clickhouse.query('DROP TABLE IF EXISTS session_temp').toPromise();
								console.log('4 1', r);

								let r12 = await clickhouse.query('DROP TABLE IF EXISTS session_temp2').toPromise();
								console.log('4 1 2', r12);

								let r2 = await clickhouse.query('CREATE TABLE session_temp (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
								console.log('4 2', r2);

								let r21 = await clickhouse.query('CREATE TABLE session_temp2 (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
								console.log('4 2 1', r21);

								let rows = await clickhouse.query(query).toPromise();
								console.log('rows len', rows.length);

								let r3 = await clickhouse.insert(
									'INSERT INTO session_temp', rows.map(r => [r.number])
								).toPromise();
								console.log('4 3', r3);

								let rows2 = await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
								console.log('rows2', rows2);
								if (rows2[0].count !== rows.length) throw new Error(`4 3: ${rows2[0].count} !== ${rows.length}`);

								let r4 = await clickhouse.query('TRUNCATE TABLE session_temp').toPromise();
								console.log('4 4', r4);

								let rows22 = await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
								if (rows22[0].count !== 0) throw new Error(`4 4: ${rows22[0].count} !== ${0}`);

								let s = clickhouse.insert('INSERT INTO session_temp').stream();
								const INSERT_ROW_COUNT = rowCount;
								let count = 0;

								for(let i = 1; i <= INSERT_ROW_COUNT; i++) {
									await s.writeRow(
										[
											e._.range(0, 50).map(
												j => `${i}:${i * 2}:${j}`
											).join('-')
										]
									);
									++count;
								}
								let r5 = await s.exec();
								console.log('4 5', r5);

								if (INSERT_ROW_COUNT !== count) throw new Error(`4 5: ${INSERT_ROW_COUNT} !== ${count}`);

								clickhouse.isUseGzip = true;
								let ts = Date.now();
								let rs = clickhouse.query(query).stream();

								let tf = new stream.Transform({
									objectMode : true,
									transform  : function (chunk, enc, cb) {
										cb(null, JSON.stringify(chunk) + '\n');
									}
								});

								clickhouse.sessionId = Date.now();
								let ws = clickhouse.insert('INSERT INTO session_temp2').stream();

								let f2 = await rs.pipe(tf).pipe(ws).exec();
								console.log('4 6', f2, Date.now() - ts, 'ms');
								clickhouse.isUseGzip = false;

								let rows41 = await clickhouse.query('SELECT count(*) AS count FROM session_temp').toPromise();
								let rows42 = await clickhouse.query('SELECT count(*) AS count FROM session_temp2').toPromise();
								if (rows41[0].count !== rows42[0].count) throw new Error(`4 6: ${rows41[0].count} !== ${rows42[0].count}`);


								try {
									let r6 = await clickhouse.query('DROP TABLE session_temp').toPromise();
									console.log('4 7', r6);

									await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
								} catch (err) {
									if (err.code !== 60) throw new Error(`4 7: ${err.code} !== 60`);
								}

								try {
									let r6 = await clickhouse.query('DROP TABLE session_temp2').toPromise();
									console.log('4 8', r6);

									await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp2').toPromise();
								} catch (err) {
									if (err.code !== 60) throw new Error(`4 8: ${err.code} !== 60`);
								}

								cb();
							}
						],
						cb
					);
				},
				cb
			);
		}
	],
	ok(function() {
		process.exit();
	})
);

```
