const
	stream         = require('stream'),
	expect         = require('expect.js'),
	_              = require('lodash'),
	{ ClickHouse } = require('../.');


const
	clickhouse = new ClickHouse({debug: false}),
	rowCount   = _.random(50 * 1024, 1024 * 1024)
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
		
		for(let query of sqlList) {
			let r = await clickhouse.query(sql).toPromise();
			
			expect(r).to.be.ok();
		}
	});
});

describe('Select', () => {
	it('use callback', callback => {
		clickhouse.query(sql).exec((err, rows) => {
			expect(err).to.not.be.ok();

			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' })

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
			})
	});


	it('use stream with pause/resume', function(callback) {
		const
			count = 10,
			pause = 1000;
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

	// 	expect(i).to.be(rowCount);
	// });

	it('use promise and await/async', async () => {
		let rows = await clickhouse.query(sql).toPromise();

		expect(rows).to.have.length(rowCount);
		expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
	});
	
										
	it('use select with external', async () => {
		let result = await clickhouse.query('SELECT count(*) AS count FROM temp_table', {
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
		expect(result[0].count).to.be(rowCount)
	});	
});


describe('session', () => {
	it('use session', async () => {
		let sessionId = clickhouse.sessionId;
		clickhouse.sessionId = Date.now();

		let result = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result).to.be.ok();
		
		clickhouse.sessionId = Date.now();
		let result2 = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result2).to.be.ok();

		clickhouse.sessionId = sessionId;
	});	
});


describe('queries', () => {
	it('queries', async () => {
		let result = await clickhouse.query('DROP TABLE IF EXISTS session_temp').toPromise();
		expect(result).to.be.ok();
		
		let result2 = await clickhouse.query('DROP TABLE IF EXISTS session_temp2').toPromise();
		expect(result2).to.be.ok();
		
		let result3 = await clickhouse.query('CREATE TABLE session_temp (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result3).to.be.ok();
		
		let result4 = await clickhouse.query('CREATE TABLE session_temp2 (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result4).to.be.ok();
		
		let data = _.range(0, rowCount).map(r => [r]);
		let result5 = await clickhouse.insert(
			'INSERT INTO session_temp', data 
		).toPromise();
		expect(result5).to.be.ok();

		let rows = await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
		expect(rows).to.be.ok();
		expect(rows).to.have.length(1);
		expect(rows[0]).to.have.key('count');
		expect(rows[0].count).to.be(data.length);
		expect(rows[0].count).to.be(rowCount);
		
		let result6 = await clickhouse.query('TRUNCATE TABLE session_temp').toPromise();
		expect(result6).to.be.ok();
		
		let ws = clickhouse.insert('INSERT INTO session_temp').stream();
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
		let result7 = await ws.exec();
		expect(result7).to.be.ok();
		expect(count).to.be(rowCount);
		
		clickhouse.isUseGzip = true;
		let ts = Date.now();
		let rs = clickhouse.query(sql).stream();
		
		let tf = new stream.Transform({
			objectMode : true,
			transform  : function (chunk, enc, cb) {
				cb(null, JSON.stringify(chunk) + '\n');
			}
		});
		
		clickhouse.sessionId = Date.now();
		let ws2 = clickhouse.insert('INSERT INTO session_temp2').stream();
		
		let result8 = await rs.pipe(tf).pipe(ws2).exec();
		expect(result8).to.be.ok();
		clickhouse.isUseGzip = false;
		
		let result9 = await clickhouse.query('SELECT count(*) AS count FROM session_temp').toPromise();
		let result10 = await clickhouse.query('SELECT count(*) AS count FROM session_temp2').toPromise();
		expect(result9).to.eql(result10);
	});
});



describe('response codes', () => {
	it('table is not exists', async () => {	
		try {
			let result = await clickhouse.query('DROP TABLE session_temp').toPromise();
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