'use strict';

const stream = require('stream');
const expect = require('expect.js');
const _  = require('lodash');
const https = require('https');
const fs = require('fs');

const { ClickHouse } = require('../.');

const database = 'test_' + _.random(1000, 100000);

const
	config     = {
		debug: false,
	},
	clickhouse = new ClickHouse({
		...config,
		database : database,
	}),
	minRnd     = 50 * 1024,
	rowCount   = _.random(minRnd, 128 * 1024),
	sql        = `SELECT
				number,
				toString(number * 2) AS str,
				toDate(number + 1) AS date
			FROM system.numbers
			LIMIT ${rowCount}`;

before(async () => {
	const temp = new ClickHouse(config);
	
	await temp.query(`DROP DATABASE IF EXISTS ${database}`).toPromise();
	await temp.query(`CREATE DATABASE ${database}`).toPromise();
});

describe('Exec', () => {
	it('should return not null object', async () => {
		const sqlList = [
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
			
			'OPTIMIZE TABLE session_temp PARTITION 201807 FINAL',
			
			`
				SELECT
					*
				FROM session_temp
				LIMIT 10
			`,
		];
		
		for(const query of sqlList) {
			const r = await clickhouse.query(query).toPromise();
			
			expect(r).to.be.ok();
		}
	});
});

describe('Select', () => {
	it('callback #1', callback => {
		clickhouse.query(sql).exec((err, rows) => {
			expect(err).to.not.be.ok();
			
			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			
			callback();
		});
	});
	
	it('callback #2', callback => {
		clickhouse.query(sql, (err, rows) => {
			expect(err).to.not.be.ok();
			
			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			
			callback();
		});
	});
	
	it('promise and await/async', async () => {
		const rows = await clickhouse.query(sql).toPromise();
		
		expect(rows).to.have.length(rowCount);
		expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
	});
	
	it('stream', function(callback) {
		let i = 0;
		let error = null;
		
		clickhouse.query(sql).stream()
			.on('data', () => ++i)
			.on('error', err => error = err)
			.on('end', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be(rowCount);
				
				callback();
			});
	});
	
	it('stream with pause/resume', function(callback) {
		const
			count = 10,
			pause = 1000,
			ts    = Date.now();
		
		this.timeout(count * pause * 2);
		
		let i     = 0,
			error = null;
		
		const stream = clickhouse.query(`SELECT number FROM system.numbers LIMIT ${count}`).stream();
		
		stream
			.on('data', () => {
				++i;
				
				stream.pause();
				
				setTimeout(() => stream.resume(), pause);
			})
			.on('error', err => error = err)
			.on('end', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be(count);
				expect(Date.now() - ts).to.be.greaterThan(count * pause);
				
				callback();
			})
	});
	
	const nodeVersion = process.version.split('.')[0].substr(1);
	if (parseInt(nodeVersion, 10) >= 10) {
		it('async for', async function() {
			let i = 0;
			
			for await (const row of clickhouse.query(sql).stream()) {
				++i;
				expect(row).to.have.key('number');
				expect(row).to.have.key('str');
				expect(row).to.have.key('date');
			}
			
			expect(i).to.be(rowCount);
		});
	}
	
	it('select with external', async () => {
		const result = await clickhouse.query(
			'SELECT count(*) AS count FROM temp_table',
			{
				external: [
					{
						name: 'temp_table',
						data: _.range(0, rowCount).map(i => `str${i}`)
					},
				]
			}
		).toPromise();
		
		expect(result).to.be.ok();
		expect(result).to.have.length(1);
		expect(result[0]).to.have.key('count');
		expect(result[0].count).to.be(rowCount);
	});
	
	it('select with external && join', async () => {
		const result = await clickhouse.query(
			`
				SELECT
					*
				FROM system.numbers AS i
				LEFT JOIN (
					SELECT
						number
					FROM system.numbers
					WHERE number IN temp_table
					LIMIT 10
				) AS n ON(n.number = i.number)
				LIMIT 100
			`,
			{
				external: [
					{
						name: 'temp_table',
						data: _.range(0, 10),
						structure: 'i UInt64'
					},
				]
			}
		).toPromise();
		
		expect(result).to.be.ok();
		expect(result).to.have.length(100);
	});
	
	it('catch error', async () => {
		try {
			await clickhouse.query(sql + '1').toPromise();
		} catch (err) {
			expect(err).to.be.ok();
		}
	});
	
	[
		{
			format: 'fake_name',
			fullFormatExpr: 'FakeName',
		},
		...ClickHouse.getFormats(),
	].forEach(({ format, fullFormatExpr }) => {
		
		// The string "foRmat" is used because different forms of writing are found in real code.
		const fullSql = sql + (format === 'fake_name' ? '' : ` foRmat ${fullFormatExpr}`);
		
		it(`with "${fullFormatExpr}" into sql`, async () => {
			const rows = await clickhouse.query(fullSql).toPromise();
			
			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
		});
		
		if (format !== 'fake_name') {
			it(`with "${fullFormatExpr}" in options`, async () => {
				const rows = await clickhouse.query(sql, {}, { format }).toPromise();
				
				expect(rows).to.have.length(rowCount);
				expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			});
		}
		
		it(`with promise "${fullFormatExpr}"`, async () => {
			try {
				await clickhouse.query(`SELECT * FROM random_table_name ${fullFormatExpr}`).toPromise();
			} catch (err) {
				expect(err).to.be.ok();
			}
		});
		
		it(`with stream ${fullFormatExpr}"`, cb => {
			let i     = 0,
				error = null;
			
			const stream = clickhouse.query(`SELECT * FROM random_table_name ${fullFormatExpr}`).stream();
			
			stream
				.on('data', () => ++i)
				.on('error', err => error = err)
				.on('close', () => {
					expect(error).to.be.ok();
					expect(error.toString()).to.match(new RegExp(`Table ${database}.random_table_name doesn\'t exist`));
					
					expect(i).to.eql(0);
					
					cb();
				})
				.on('end', () => {
					cb(new Error('no way #2'));
				});
		});
	});
});

describe('session', () => {
	it('use session', async () => {
		const sessionId = clickhouse.sessionId;
		clickhouse.sessionId = Date.now();
		
		const result = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result).to.be.ok();
		
		clickhouse.sessionId = Date.now();
		const result2 = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result2).to.be.ok();
		
		clickhouse.sessionId = sessionId;
	});
  
  it('uses session per query', async () => {
		let tempSessionId = `${Date.now()}`;
		
		const result = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`, {}, {sessionId: tempSessionId}
		).toPromise();
		expect(result).to.be.ok();
		
		const result2 = await clickhouse.query(
			`SELECT * FROM test_table LIMIT 10`, {}, {sessionId: tempSessionId}
		).toPromise();
        expect(result2).to.be.ok();
        try {
            await clickhouse.query(
                `SELECT * FROM test_table LIMIT 10`, {}, {sessionId: `${tempSessionId}_bad`}
            ).toPromise();
        } catch (error) {
            expect(error.code).to.be(60);
        }
        
        const result3 = await clickhouse.query(
			`DROP TEMPORARY TABLE test_table`, {}, {sessionId: tempSessionId}
		).toPromise();
		expect(result3).to.be.ok();
  });

	it('use setSessionPerQuery #1', async () => {
		const sessionPerQuery = clickhouse.sessionPerQuery;
		clickhouse.setSessionPerQuery(false);

		const result = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result).to.be.ok();

		try {
			await clickhouse.query(
				`CREATE TEMPORARY TABLE test_table
				(_id String, str String)
				ENGINE=Memory`
			).toPromise();

			expect(1).to.be(0);
		} catch (err) {
			expect(err).to.be.ok();
			expect(err.code).to.be(57);
		}

		clickhouse.setSessionPerQuery(sessionPerQuery);
	});
	
	it('use setSessionPerQuery #2', async () => {
		const sessionPerQuery = clickhouse.sessionPerQuery;
		clickhouse.setSessionPerQuery(true);
		
		const result = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result).to.be.ok();
		
		await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
				(_id String, str String)
				ENGINE=Memory`
		).toPromise();
		
		clickhouse.setSessionPerQuery(sessionPerQuery);
	});
});

// You can use all settings from request library (https://github.com/request/request#tlsssl-protocol)
// Generate ssl file with:
// sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout test/cert/server.key -out test/cert/server.crt
describe('TLS/SSL Protocol', () => {
	it('use TLS/SSL Protocol', async () => {
		let server = null;

		try {
			server = https.createServer(
				{
					key  : fs.readFileSync('test/cert/server.key'),
					cert : fs.readFileSync('test/cert/server.crt'),
				},
				(req, res) => {
					res.writeHead(200);
					res.end('{\n\t"meta":\n\t[\n\t\t{\n\t\t\t"name": "plus(1, 1)",\n\t\t\t"type": "UInt16"\n\t\t}\n\t],\n\n\t"data":\n\t[\n\t\t{\n\t\t\t"plus(1, 1)": 2\n\t\t}\n\t],\n\n\t"rows": 1,\n\n\t"statistics":\n\t{\n\t\t"elapsed": 0.000037755,\n\t\t"rows_read": 1,\n\t\t"bytes_read": 1\n\t}\n}\n');
				})
				.listen(8000);

			const temp = new ClickHouse({
				...config,
				url       : 'https://localhost:8000',
				port      : 8000,
				reqParams : {
					agentOptions: {
						ca: fs.readFileSync('test/cert/server.crt'),
						cert: fs.readFileSync('test/cert/server.crt'),
						key: fs.readFileSync('test/cert/server.key'),
					}
				},
				debug    : false,
			});

			const r = await temp.query('SELECT 1 + 1 Format JSON').toPromise();
			expect(r).to.be.ok();
			expect(r[0]).to.have.key('plus(1, 1)');
			expect(r[0]['plus(1, 1)']).to.be(2);

			if (server) {
				server.close();
			}
		} catch(err) {
			if (server) {
				server.close();
			}

			throw err;
		}
	});
});

describe('queries', () => {
	it('insert field as array', async () => {
		clickhouse.sessionId = Date.now();
		
		const r = await clickhouse.query(`
			CREATE TABLE IF NOT EXISTS test_array (
				date Date,
				str String,
				arr Array(String),
				arr2 Array(Date),
				arr3 Array(UInt8)
			) ENGINE=MergeTree(date, date, 8192)
		`).toPromise();
		expect(r).to.be.ok();
		
		const rows = [
			{
				date: '2018-01-01',
				str: 'Вам, проживающим за оргией оргию,',
				arr: [],
				arr2: ['1915-01-02', '1915-01-03'],
				arr3: [1,2,3,4,5]
			},
			
			{
				date: '2018-02-01',
				str: 'имеющим ванную и теплый клозет!',
				arr: ['5670000000', 'asdas dasf'],
				arr2: ['1915-02-02'],
				arr3: []
			}
		];
		
		const r2 = await clickhouse.insert(
			'INSERT INTO test_array (date, str, arr, arr2, arr3)',
			rows
		).toPromise();
		expect(r2).to.be.ok();
	});
	
	it('select, insert and two pipes', async () => {
		const result = await clickhouse.query('DROP TABLE IF EXISTS session_temp').toPromise();
		expect(result).to.be.ok();
		
		const result2 = await clickhouse.query('DROP TABLE IF EXISTS session_temp2').toPromise();
		expect(result2).to.be.ok();
		
		const result3 = await clickhouse.query('CREATE TABLE session_temp (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result3).to.be.ok();
		
		const result4 = await clickhouse.query('CREATE TABLE session_temp2 (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result4).to.be.ok();
		
		const data = _.range(0, rowCount).map(r => [r]);
		const result5 = await clickhouse.insert(
			'INSERT INTO session_temp', data
		).toPromise();
		expect(result5).to.be.ok();
		
		const rows = await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
		expect(rows).to.be.ok();
		expect(rows).to.have.length(1);
		expect(rows[0]).to.have.key('count');
		expect(rows[0].count).to.be(data.length);
		expect(rows[0].count).to.be(rowCount);
		
		const result6 = await clickhouse.query('TRUNCATE TABLE session_temp').toPromise();
		expect(result6).to.be.ok();
		
		const ws = clickhouse.insert('INSERT INTO session_temp').stream();
		let count = 0;
		
		for(let i = 1; i <= rowCount; i++) {
			await ws.writeRow(
				[
					_.range(0, 50).map(
						j => `${i}:${i * 2}:${j}`
					).join('-')
				]
			);
			++count;
		}
		const result7 = await ws.exec();
		expect(result7).to.be.ok();
		expect(count).to.be(rowCount);
		
		clickhouse.isUseGzip = true;
		const rs = clickhouse.query(sql).stream();
		
		const tf = new stream.Transform({
			objectMode : true,
			transform  : function (chunk, enc, cb) {
				cb(null, JSON.stringify(chunk) + '\n');
			}
		});
		
		clickhouse.sessionId = Date.now();
		const ws2 = clickhouse.insert('INSERT INTO session_temp2').stream();
		
		const result8 = await rs.pipe(tf).pipe(ws2).exec();
		expect(result8).to.be.ok();
		clickhouse.isUseGzip = false;
		
		const result9 = await clickhouse.query('SELECT count(*) AS count FROM session_temp').toPromise();
		const result10 = await clickhouse.query('SELECT count(*) AS count FROM session_temp2').toPromise();
		expect(result9).to.eql(result10);
	});
	
	it('insert && errors', async () => {
		try {
			const stream = clickhouse.insert('INSERT INTO BAD_TABLE').stream();
			
			for (let i = 0; i < 10; i++) {
				stream.writeRow([i, `test: ${i}`]);
			}
			
			await stream.exec();
			
			// no way!
			expect(1).to.be.equal(0);
		} catch (err) {
			expect(err).to.be.ok();
			expect(err.code).to.be(60);
		}
	});
	
	it('select number as number', async () => {
		const result = await clickhouse.query('DROP TABLE IF EXISTS test_int_temp').toPromise();
		expect(result).to.be.ok();
		
		const result1 = await clickhouse.query('CREATE TABLE test_int_temp (int_value Int8 ) ENGINE=Memory').toPromise();
		expect(result1).to.be.ok();
		
		const int_value_data = [{int_value: 0}];
		const result3 = await clickhouse.insert('INSERT INTO test_int_temp (int_value)', int_value_data).toPromise();
		expect(result3).to.be.ok();
		
		const result4 = await clickhouse.query('SELECT int_value FROM test_int_temp').toPromise();
		expect(result4).to.eql(int_value_data);
	});
});

describe('response codes', () => {
	it('table is not exists', async () => {
		try {
			const result = await clickhouse.query('DROP TABLE session_temp').toPromise();
			expect(result).to.be.ok();
			
			await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
			expect().fail('You should not be here');
		} catch (err) {
			expect(err).to.be.ok();
			expect(err).to.have.key('code');
			expect(err.code).to.be(60);
		}
		
		try {
			let result = await clickhouse.query('DROP TABLE session_temp2').toPromise();
			expect(result).to.be.ok();
		
			await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp2').toPromise();
			expect().fail('You should not be here2');
		} catch (err) {
			expect(err).to.be.ok();
			expect(err).to.have.key('code');
			expect(err.code).to.be(60);
		}
	});
});



describe('set database', () => {
	it('create instance with non-default database', async () => {
		const noDefaultDb = 'default_' + _.random(1000, 10000);
		const r = await clickhouse.query(`CREATE DATABASE ${noDefaultDb}`).toPromise();
		expect(r).to.be.ok();
		
		const temp = new ClickHouse({
			...config,
			database: noDefaultDb,
		});
		
		
		const result3 = await temp.query('CREATE TABLE session_temp (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result3).to.be.ok();
		
		const r2 = await temp.query(`DROP DATABASE ${noDefaultDb}`).toPromise();
		expect(r2).to.be.ok();
	});
});


describe('compatibility with Sequelize ORM', () => {
	it('select with ;', async () => {
		const sqls = [
			'SELECT 1 + 1',
			'SELECT 1 + 1;',
			'SELECT 1 + 1 ;',
			'SELECT 1 + 1 ; '
		];

		for(const sql of sqls) {
			const r = await clickhouse.query(sql).toPromise();
			expect(r).to.be.ok();
		}
	});
});



describe('Constructor options', () => {
	const addConfigs = [
		{
			url: 'localhost',
		},

		{
			url: 'http://localhost',
		},

		{
			url: 'http://localhost:8123',
			port: 8123,
		},

		{
			host: 'localhost',
		},

		{
			host: 'http://localhost',
		},

		{
			host: 'http://localhost:8123',
			port: 8124,
		},

		{
			host: 'http://localhost:8123',
			port: 8124,
			format: "json",
		},

		{
			host: 'http://localhost:8123',
			port: 8124,
			format: "tsv",
		},

		{
			host: 'http://localhost:8123',
			port: 8124,
			format: "csv",
		},
	];

	for(const addConfig of addConfigs) {
		it(`url and host (${JSON.stringify(addConfig)})`, async () => {
			const clickhouse = new ClickHouse({
				...config,
				...addConfig,
			});

			const r = await clickhouse.query('SELECT 1 + 1').toPromise();
			expect(r).to.be.ok();
		});
	}
	
	
	it('user && password ok', async () => {
		const clickhouses = [
			new ClickHouse({
				...config,
				user: 'default',
				password: ''
			}),
			
			new ClickHouse({
				...config,
				username: 'default',
				password: ''
			}),

			new ClickHouse({
				...config,
				basicAuth: {
					username: 'default',
					password: ''
				}
			}),
		];
		
		for(const clickhouse of clickhouses) {
			const r = await clickhouse.query('SELECT 1 + 1').toPromise();
			expect(r).to.be.ok();
		}
	});
	
	
	it('user && password fail', async () => {
		const clickhouses = [
			new ClickHouse({
				...config,
				user: 'default1',
				password: ''
			}),
			
			new ClickHouse({
				...config,
				username: 'default1',
				password: ''
			}),
		];
		
		for(const clickhouse of clickhouses) {
			try {
				await clickhouse.query('SELECT 1 + 1').toPromise();
			} catch (err) {
				expect(err).to.be.ok();
			}
		}
	});
	
	
	it('database', async () => {
		const clickhouses = [
			new ClickHouse({
				...config,
				...{
					config: {
						database: 'system',
					}
				},
			}),
			
			new ClickHouse({
				...config,
				database: 'system',
			}),
		];
		
		for(const clickhouse of clickhouses) {
			const [{ count }] = await clickhouse.query(
				'SELECT count(*) AS count FROM (SELECT number FROM numbers LIMIT 1024)'
			).toPromise();
			
			expect(count).to.be(1024);
		}
	});
	
});


describe('Exec system queries', () => {
	it('select with ;', async () => {
		const sqls = [
			'EXISTS TABLE test_db.myTable'
		];

		for(const sql of sqls) {
			const [ row ] = await clickhouse.query(sql).toPromise();
			expect(row).to.be.ok();
			expect(row).to.have.key('result');
			expect(row.result).to.be(0);
		}
	});
});

describe('Select and WITH TOTALS statement', () => {
	[false, true].forEach(withTotals => {
		it(`is ${withTotals}`, async () => {
			const query = clickhouse.query(`
				SELECT
					number % 3 AS i,
					groupArray(number) as kList
				FROM (
					SELECT number FROM system.numbers LIMIT 14
				)
				GROUP BY i ${withTotals ? '' : 'WITH TOTALS'}
				FORMAT TabSeparatedWithNames
			`);
			
			if (withTotals) {
				query.withTotals();
			}
			
			const result = await query.toPromise();
			
			expect(result).to.have.key('meta');
			expect(result).to.have.key('data');
			expect(result).to.have.key('totals');
			expect(result).to.have.key('rows');
			expect(result).to.have.key('statistics');
		});
	});
	
	it('WITH TOTALS #2', async () => {
		const LIMIT_COUNT = 10;
		
		const result = await clickhouse.query(`
			SELECT
				rowNumberInAllBlocks() AS i,
				SUM(number)
			FROM (
				SELECT
					number
				FROM
					system.numbers
				LIMIT 1000
			)
			GROUP BY i WITH TOTALS
			LIMIT ${LIMIT_COUNT}
		`).toPromise();
		
		expect(result).to.have.key('meta');
		expect(result).to.have.key('data');
		expect(result).to.have.key('totals');
		expect(result).to.have.key('rows');
		expect(result.rows).to.be(LIMIT_COUNT);
		expect(result).to.have.key('statistics');
	});
	
	it('WITH TOTALS #3 (JSON)', async () => {
		const LIMIT_COUNT = 10;
		
		const result = await clickhouse.query(`
			SELECT
			  rowNumberInAllBlocks() AS i,
			  number
			FROM (
			  SELECT
			    number
			  FROM
			    system.numbers
			  LIMIT 1000
			)
			LIMIT ${LIMIT_COUNT}
			FORMAT JSON
		`).withTotals().toPromise();
		
		expect(result).to.have.key('meta');
		expect(result).to.have.key('data');
		expect(result).to.have.key('rows');
		expect(result.rows).to.be(LIMIT_COUNT);
		expect(result).to.have.key('statistics');
	});
});

describe('Abort query', () => {
	it('exec & abort', cb => {
		const $q = clickhouse.query(`SELECT number FROM system.numbers LIMIT ${rowCount}`);
		
		let i     = 0,
			error = null;
		
		const stream = $q.stream()
			.on('data', () => {
				++i;
				
				if (i > minRnd) {
					stream.pause();
				}
			})
			.on('error', err => error = err)
			.on('close', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be.below(rowCount);
				
				// Take some time for waiting of cancel query
				setTimeout(() => cb(), 4 * 1000);
			})
			.on('end', () => {
				cb(new Error('no way! May be stream very quick!'));
			});
		
		setTimeout(() => $q.destroy(), 10 * 1000);
	});
});

after(async () => {
	await clickhouse.query(`DROP DATABASE IF EXISTS ${database}`).toPromise();
});
