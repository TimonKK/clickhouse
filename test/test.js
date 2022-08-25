'use strict';

const stream = require('stream');
const expect = require('expect.js');
const _  = require('lodash');
const https = require('https');
const fs = require('fs');
const path = require('path');

const { ClickHouse } = require('../.');

const database = 'test_' + _.random(1000, 100000);

const
	configFilepath = process.env.CLICKHOUSE_TEST_CONF_FILE || './test_config.json',
    configPath = path.resolve(process.cwd(), configFilepath),
    extConfig = fs.existsSync(configPath) ? JSON.parse(fs.readFileSync(configPath, { encoding: 'utf-8' })) : undefined,
	config     = {
		...extConfig,
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

describe.skip('On cluster', () => {
	// Note: this test only works with ClickHouse setup as Cluster named test_cluster
	it('should be able to create and drop a table', async () => {
		const createTableQuery = `
			CREATE TABLE ${database}.test_on_cluster ON CLUSTER test_cluster (
				test String
			)
			ENGINE=MergeTree ORDER BY test;`;
	  	const createTableQueryResult = await clickhouse.query(createTableQuery).toPromise();
	  	expect(createTableQueryResult).to.be.ok();

		const dropTableQuery = `
			DROP TABLE ${database}.test_on_cluster ON CLUSTER test_cluster;`;
	  	const dropTableQueryResult = await clickhouse.query(dropTableQuery).toPromise();
	  	expect(dropTableQueryResult).to.be.ok();
	});
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

	it('streams should handle network error', function(callback) {
		let i = 0;
		const host = 'non-existing-clickhouse-server'; // to simulate a dns failure

		new ClickHouse({
			...config,
			host
		}).query('SELECT number FROM system.numbers LIMIT 10').stream()
			.on('data', () => ++i)
			.on('error', error => {
				expect(error.code).to.be.equal('ENOTFOUND');
				expect(error.syscall).to.be.equal('getaddrinfo');
				expect(error.hostname).to.be.equal(host);
				expect(i).to.be(0);				
				callback();
			});
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

	it('parameterized ', callback => {

		let params_sql =  `SELECT
			number,
			toString(number * 2) AS str,
			toDate(number + 1) AS date
		FROM numbers(10)
		WHERE number = {num:UInt64}
		  OR number IN {nums:Array(UInt64)}`;

		let params_data = {
			params: {
				num: 0,
				nums: [1,2]
			}
		}

		clickhouse.query(params_sql, params_data).exec((err, rows) => {
			expect(err).to.not.be.ok();
			
			expect(rows).to.have.length(3);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			
			callback();
		});
	});

});

(extConfig? describe.skip : describe)('session', () => {
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

(extConfig? describe.skip : describe)('TLS/SSL Protocol', () => {
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
	it('default HTTPS port is 433', async () => {
		const clickhouse = new ClickHouse({
			...config,
			url       : 'https://localhost'
		});
		expect(clickhouse.opts.url).to.match(/localhost\//);
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
				arr3 Array(UInt8),
				id1 UUID
			) ENGINE=MergeTree(date, date, 8192)
		`).toPromise();
		expect(r).to.be.ok();
		
		const rows = [
			{
				date: '2018-01-01',
				str: 'Вам, проживающим за оргией оргию,',
				arr: [],
				arr2: ['1985-01-02', '1985-01-03'],
				arr3: [1,2,3,4,5],
				id1: '102a05cb-8aaf-4f11-a442-20c3558e4384'
			},
			
			{
				date: '2018-02-01',
				str: 'It\'s apostrophe test.',
				arr: ['5670000000', 'asdas dasf. It\'s apostrophe test.'],
				arr2: ['1985-02-02'],
				arr3: [],
				id1: 'c2103985-9a1e-4f4a-b288-b292b5209de1'
			}
		];
		
		const r2 = await clickhouse.insert(
			`insert into test_array 
			(date, str, arr, arr2, 
			 arr3, id1)`,
			rows
		).toPromise();
		expect(r2).to.be.ok();
		const r3 = await clickhouse.query('SELECT * FROM test_array ORDER BY date').toPromise();		
		expect(r3).to.eql(rows);
	});

	it('insert field as raw string', async () => {
		clickhouse.sessionId = Date.now();
		
		const r = await clickhouse.query(`
			CREATE TABLE IF NOT EXISTS test_raw_string (
				date Date,
				str String,
				arr Array(String),
				arr2 Array(Date),
				arr3 Array(UInt8),
				fixedStr String
			) ENGINE=MergeTree(date, date, 8192)
		`).toPromise();
		expect(r).to.be.ok();
		
		const rows = [
			'(\'2018-01-01 10:00:00\',\'Вам, проживающим за оргией оргию,\',[],[\'1915-01-02 10:00:00\',\'1915-01-03 10:00:00\'],[1,2,3,4,5],unhex(\'60ed56e75bb93bd353267faa\'))',
			'(\'2018-02-01 10:00:00\',\'имеющим ванную и теплый клозет! It\'\'s apostrophe test.\',[\'5670000000\',\'asdas dasf\'],[\'1915-02-02 10:00:00\'],[],unhex(\'60ed56f4a88cd5dcb249d959\'))'
		];
		
		const r2 = await clickhouse.insert(
			'INSERT INTO test_raw_string (date, str, arr, arr2, arr3, fixedStr) VALUES',
			rows
		).toPromise();
		expect(r2).to.be.ok();
	});
	
	it('insert stream accept raw string', async () => {
		clickhouse.sessionId = Date.now();
		
		const r = await clickhouse.query(`
			CREATE TABLE IF NOT EXISTS test_insert_stream_raw_string (
				date Date,
				str String,
				arr Array(String),
				arr2 Array(Date),
				arr3 Array(UInt8),
				fixedStr FixedString(12)
			) ENGINE=MergeTree(date, date, 8192)
		`).toPromise();
		expect(r).to.be.ok();
		
		const rows = [
			'(\'2018-01-01 10:00:00\',\'Вам, проживающим за оргией оргию,\',[],[\'1915-01-02 10:00:00\',\'1915-01-03 10:00:00\'],[1,2,3,4,5],unhex(\'60ed56e75bb93bd353267faa\'))',
			'(\'2018-02-01 10:00:00\',\'имеющим ванную и теплый клозет!\',[\'5670000000\',\'asdas dasf\'],[\'1915-02-02 10:00:00\'],[],unhex(\'60ed56f4a88cd5dcb249d959\'))'
		];
		
		const stream = await clickhouse.insert(
			'INSERT INTO test_insert_stream_raw_string (date, str, arr, arr2, arr3, fixedStr) VALUES',
		).stream();
		stream.writeRow(rows[0]);
		stream.writeRow(rows[1]);
		const r2 = await stream.exec();
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

	it('insert with params', async () => {
		const result = await clickhouse.query('DROP TABLE IF EXISTS test_par_temp').toPromise();
		expect(result).to.be.ok();
		
		const result1 = await clickhouse.query(`CREATE TABLE test_par_temp (
			int_value UInt32, 
			str_value1 String, 
			str_value2 String, 
			date_value Date, 
			date_time_value DateTime, 
			decimal_value Decimal(10,4),
			arr Array(String),
			arr2 Array(Date),
			arr3 Array(UInt32)
		) ENGINE=Memory`).toPromise();
		expect(result1).to.be.ok();
		
		const row = {
			int_value: 12345,
			str_value1: 'Test for "masked" characters. It workes, isn\'t it?',
			str_value2: JSON.stringify({name:'It is "something".'}),
			date_value: '2022-08-18',
			date_time_value: '2022-08-18 19:07:00',
			decimal_value: 1234.678,
			arr: ['asdfasdf', 'It\'s apostrophe test'],
			arr2: ['2022-01-01', '2022-10-10'],
			arr3: [12345, 54321],
		};
		const result2 = await clickhouse.insert(`INSERT INTO test_par_temp (int_value, str_value1, str_value2, date_value, date_time_value,	decimal_value,
			arr, arr2, arr3)
			VALUES ({int_value:UInt32}, {str_value1:String}, {str_value2:String}, {date_value:Date}, {date_time_value:DateTime}, {decimal_value: Decimal(10,4)},
			{arr:Array(String)},{arr2:Array(Date)},{arr3:Array(UInt32)})`, 
			{params: {
				...row,
				decimal_value: row.decimal_value.toFixed(4)
				}
			}).toPromise();
		expect(result2).to.be.ok();
		
		const result3 = await clickhouse.query('SELECT * FROM test_par_temp').toPromise();		
		expect(result3).to.eql([row]);
	});

	it('insert select', async () => {
		const result = await clickhouse.query('DROP TABLE IF EXISTS test_par_temp').toPromise();
		expect(result).to.be.ok();
		
		const result1 = await clickhouse.query(`CREATE TABLE test_par_temp (
			int_value UInt32, 
			str_value1 String, 
			str_value2 String, 
			date_value Date, 
			date_time_value DateTime, 
			decimal_value Decimal(10,4),
			arr Array(String),
			arr2 Array(Date),
			arr3 Array(UInt32)
		) ENGINE=Memory`).toPromise();
		expect(result1).to.be.ok();
		
		const row = {
			int_value: 12345,
			str_value1: 'Test for "masked" characters. It workes, isn\'t it?',
			str_value2: JSON.stringify({name:'It is "something".'}),
			date_value: '2022-08-18',
			date_time_value: '2022-08-18 19:07:00',
			decimal_value: 1234.678,
			arr: ['asdfasdf', 'It\'s apostrophe test'],
			arr2: ['2022-01-01', '2022-10-10'],
			arr3: [12345, 54321],
		};
		const result2 = await clickhouse.insert(`INSERT INTO test_par_temp (int_value, str_value1, str_value2, date_value, date_time_value,	decimal_value,
			arr, arr2, arr3)
			select {int_value:UInt32}, {str_value1:String}, {str_value2:String}, {date_value:Date}, {date_time_value:DateTime}, {decimal_value: Decimal(10,4)},
			{arr:Array(String)},{arr2:Array(Date)},{arr3:Array(UInt32)}`, 
			{params: {
				...row,
				decimal_value: row.decimal_value.toFixed(4)
				}
			}).toPromise();
		expect(result2).to.be.ok();

		const result3 = await clickhouse.query('SELECT * FROM test_par_temp').toPromise();		
		expect(result3).to.eql([row]);

		const result4 = await clickhouse.insert(`INSERT INTO test_par_temp (int_value, str_value1, str_value2, date_value, date_time_value,	decimal_value,
			arr, arr2, arr3)
			select 123456, 'awerqwerqwer', 'rweerwrrewr', '2022-08-25', '2022-08-25 02:00:01', '123.1234',
			['aaa','bbb'],['2022-08-22','2022-08-23'],[1,2,3,4]`
			).toPromise();
		expect(result2).to.be.ok();
		

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



(extConfig? describe.skip : describe)('Constructor options', () => {
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

	it('start with WITH', async() => {
		const r = await clickhouse.query(`
			WITH x as (SELECT 1) SELECT * FROM x
		`).toPromise();

		expect(r).to.be.ok();
		expect(r[0]).to.be.eql({1: 1});
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

describe('Raw response', () => {
	it('"raw" parameter should return response as a raw CSV/TSV/JSON string', async () => {
		const tableName = 'test_raw_response';
		const insertValues = [
			{id: 'fm', name: 'Freddie Mercury', age: 45},
			{id: 'js', name: 'John Lennon', age: 40},
			{id: 'ep', name: 'Elvis Presley', age: 42},
		];
		
		const createResponse = await clickhouse.query(`
			CREATE TABLE IF NOT EXISTS ${tableName} (id String, name String, age UInt8) ENGINE = MergeTree() ORDER BY id;
		`).toPromise();
		expect(createResponse).to.be.ok();

		const insertResponse = await clickhouse.insert(`INSERT INTO ${tableName} (id, name, age)`, insertValues).toPromise();
		expect(insertResponse).to.be.ok();

		for (const format of ['csv', 'tsv', 'json']) {
			const rawClient = new ClickHouse({...config, database, raw: true, format});
			const result = await rawClient.query(`SELECT * FROM ${tableName}`).toPromise();
			expect(typeof result).to.be.equal('string');

			let data = rawClient.bodyParser(result);
			format === 'json' && ({data} = data);
			expect(data.length).to.be.equal(insertValues.length);
			
			for (const insertValue of insertValues) {
				const resultValue = data.find(el => el.id === insertValue.id);
				expect(resultValue).not.to.be.empty();
				expect(resultValue).to.be.eql(insertValue);
			}
		}

	})
})

after(async () => {
	await clickhouse.query(`DROP DATABASE IF EXISTS ${database}`).toPromise();
});
