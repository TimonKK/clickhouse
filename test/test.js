const
	stream         = require('stream'),
	expect         = require('expect.js'),
	_              = require('lodash'),
	{ ClickHouse } = require('../.');


const
	clickhouse = new ClickHouse({debug: false}),
	rowCount   = _.random(50 * 1024, 1024 * 1024),
	sql        = `SELECT
				number,
				toString(number * 2) AS str,
				toDate(number + 1) AS date
			FROM system.numbers
			LIMIT ${rowCount}`;


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
			
			'OPTIMIZE TABLE session_temp PARTITION 201807 FINAL'
		];
		
		for(const query of sqlList) {
			const r = await clickhouse.query(query).toPromise();
			
			expect(r).to.be.ok();
		}
	});
});

describe('Select', () => {
	it('use callback', callback => {
		clickhouse.query(sql).exec((err, rows) => {
			expect(err).to.not.be.ok();
			
			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			
			callback();
		});
	});
	
	
	it('use stream', function(callback) {
		this.timeout(10000);
		
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
	
	
	it('use stream with pause/resume', function(callback) {
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
	
	
	// Waiting for Node.js 10
	// it('use async for', async function() {
	// 	let i = 0;
	// 	for await (const row of await clickhouse.query(sql).stream()) {
	// 		++i;
	// 		expect(row).to.have.key('number');
	// 		expect(row).to.have.key('str');
	// 		expect(row).to.have.key('date');
	// 	}
	// 
	// 	expect(i).to.be(rowCount);
	// });
	
	it('use promise and await/async', async () => {
		let rows = await clickhouse.query(sql).toPromise();
		
		expect(rows).to.have.length(rowCount);
		expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
	});
	
	
	it('use select with external', async () => {
		const result = await clickhouse.query('SELECT count(*) AS count FROM temp_table', {
			external: [
				{
					name: 'temp_table',
					data: _.range(0, rowCount).map(i => `str${i}`)
				},
			]
		}).toPromise();
		
		expect(result).to.be.ok();
		expect(result).to.have.length(1);
		expect(result[0]).to.have.key('count');
		expect(result[0].count).to.be(rowCount);
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
});


describe('queries', () => {
	it('insert field as array', async () => {
		clickhouse.sessionId = Date.now();
		await clickhouse.query('CREATE DATABASE IF NOT EXISTS test').toPromise();
		await clickhouse.query('use test').toPromise();
		await clickhouse.query(`
			CREATE TABLE IF NOT EXISTS test_array (
				date Date,
				str String,
				arr Array(String),
				arr2 Array(Date),
				arr3 Array(UInt8)
			) ENGINE=MergeTree(date, date, 8192)
		`).toPromise();
		
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
		
		
		const r = await clickhouse.insert('INSERT INTO test_array (date, arr, arr2, arr3)', rows).toPromise();
		expect(r).to.be.ok();
		
		
		clickhouse.sessionId = null;
	});
	
	
	it('queries', async () => {
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

		const result11 = await clickhouse.query('SELECT date FROM test.test_array GROUP BY date WITH TOTALS').toPromiseRaw();
		expect(result11).to.have.key('totals');	
		expect(result11).to.have.key('data');
		expect(result11).to.have.key('statistics');

		const result12 = await clickhouse.query('SELECT date, count() AS totals_count FROM test.test_array GROUP BY date WITH TOTALS').toPromiseRaw('totals');
		expect(result12).to.have.key('totals_count');
		expect(result12.totals_count).to.have.greaterThan(0);
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
		
		// try {
		// 	let result = await clickhouse.query('DROP TABLE session_temp2').toPromise();
		// 	expect(result).to.be.ok();
			
		// 	await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp2').toPromise();
		// 	expect().fail('You should not be here2');
		// } catch (err) {
		// 	expect(err).to.be.ok();
		// 	expect(err).to.have.key('code');
		// 	expect(err.code).to.be(60);
		// }
	});
});