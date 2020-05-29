'use strict';

const zlib = require('zlib');
const _ = require('lodash');
const request = require('request');
const { Transform, Readable, } = require('stream');
const JSONStream = require('JSONStream');
const through = require('through');
const stream2asynciter = require('stream2asynciter');
const { URL } = require('url');
const tsv = require('tsv');
const uuidv4 = require('uuid/v4');


/**
 * Content-Encoding: gzip
 * Accept-Encoding: gzip
 * и включить настройку ClickHouse enable_http_compression.
 *
 * session_id
 *
 * session_timeout
 */

const SEPARATORS = {
	TSV: "\t",
	CSV: ",",
	Values: ","
};

const ALIASES = {
	TabSeparated: "TSV"
};

var ESCAPE_STRING = {
	/**
	 * @return {string}
	 */
	TSV: function (value) {
		return value
			.replace(/\\/g, '\\\\')
			.replace(/\\/g, '\\')
			.replace(/\t/g, '\\t')
			.replace(/\n/g, '\\n');
	},
	
	CSV: function (value) {
		return value.replace (/\"/g, '""');
	},
};

var ESCAPE_NULL = {
	TSV: "\\N",
	CSV: "\\N",
	Values: "\\N",
	JSONEachRow: "\\N",
};

const R_ERROR = new RegExp('Code: ([0-9]{2}), .*Exception:');

const URI = 'localhost';

const PORT = 8123;

const DATABASE = 'default';
const USERNAME = 'default';

const FORMATS = {
	'json': 'JSON',
	'tsv': 'TabSeparatedWithNames',
	'csv': 'CSVWithNames',
};

const REVERSE_FORMATS = Object.keys(FORMATS).reduce(
	function(obj, format) {
		obj[FORMATS[format]] = format;
		return obj;
	},
	{}
);

const R_FORMAT_PARSER = new RegExp(
	`FORMAT (${Object.keys(FORMATS).map(k => FORMATS[k]).join('|')})`,
	'mi'
);

function parseCSV(body, options = { header: true }) {
	const data = new tsv.Parser(SEPARATORS.CSV, options).parse(body);
	data.splice(data.length - 1, 1);
	return data;
}

function parseTSV(body, options = { header: true }) {
	const data = new tsv.Parser(SEPARATORS.TSV, options).parse(body);
	data.splice(data.length - 1, 1);
	return data;
}

function parseCSVStream(s = new Set()) {
	let isFirst = true;
	let ref = {
		fields: []
	};
	
	return through(function (chunk) {
		let str = chunk.toString();
		let parsed = parseCSV(str, {header: isFirst});
		let strarr = str.split("\n");
		let plen = (isFirst && strarr.length - 1 || strarr.length) - parsed.length;
		
		if (!isFirst) {
			chunk = Buffer.concat([Buffer.from([...s].join("\n")), chunk]).toString();
			parsed = parseCSV(str, {header: isFirst});
			s = new Set();
		}
		strarr.splice(strarr.length - plen).forEach((value => s.add(value)));
		chunkBuilder.call(this, isFirst, ref, str, parsed);
		isFirst = false;
	})
}

function parseJSONStream() {
	return JSONStream.parse(['data', true]);
}

function parseTSVStream(s = new Set()) {
    let isFirst = true;
    let ref = {
		fields: []
	};
 
	return through(function (chunk) {
		let str = chunk.toString();
		let parsed = parseTSV(str, {header: isFirst});
		let strarr = str.split("\n");
		let plen = (isFirst && strarr.length - 1 || strarr.length) - parsed.length;
		
		if (!isFirst) {
			chunk = Buffer.concat([Buffer.from([...s].join("\n")), chunk]).toString();
			parsed = parseTSV(str, {header: isFirst});
			s = new Set();
		}
		strarr.splice(strarr.length - plen).forEach((value => s.add(value)));
		chunkBuilder.call(this, isFirst, ref, str, parsed);
		isFirst = false;
	});
}

function chunkBuilder(isFirst, ref, chunk, parsed) {
	if (isFirst) {
		ref.fields = Object.keys(parsed[0]);
		parsed.forEach((value) => {
			this.queue(value);
		});
	} else {
		parsed.forEach((value) => {
			let result = {};
			ref.fields.forEach((field, index) => (result[field] = value[index]));
			this.queue(result);
			result = null;
		});
	}
}

function encodeValue(quote, v, _format, isArray) {
	const format = ALIASES[_format] || _format;
	
	switch (typeof v) {
		case 'string':
			if (isArray) {
				return `'${ESCAPE_STRING[format] ? ESCAPE_STRING[format](v, quote) : v}'`;
			}
			
			return ESCAPE_STRING[format] ? ESCAPE_STRING[format](v, quote) : v;
		case 'number':
			if (isNaN(v)) {
				return 'nan';
			}
			
			if (v === +Infinity) {
				return '+inf';
			}
			
			if (v === -Infinity) {
				return '-inf';
			}
			
			if (v === Infinity) {
				return 'inf';
			}
			
			return v;
		case 'object':
			
			// clickhouse allows to use unix timestamp in seconds
			if (v instanceof Date) {
				return ("" + v.valueOf()).substr (0, 10);
			}
			
			// you can add array items
			if (v instanceof Array) {
				return '[' + v.map(function (i) {
					return encodeValue(true, i, format, true);
				}).join(',') + ']';
			}
			
			// TODO: tuples support
			if (!format) {
				console.trace();
			}
			
			if (v === null) {
				return format in ESCAPE_NULL ? ESCAPE_NULL[format] : v;
			}
			
			return format in ESCAPE_NULL ? ESCAPE_NULL[format] : v;
		case 'boolean':
			return v === true ? 1 : 0;
		default:
			return v;
	}
}

function getErrorObj(res) {
	const err = new Error(`${res.statusCode}: ${res.body || res.statusMessage}`);
	
	if (res.body) {
		const m = res.body.match(R_ERROR);
		if (m) {
			if (m[1] && isNaN(parseInt(m[1])) === false) {
				err.code = parseInt(m[1]);
			}
			
			if (m[2]) {
				err.message = m[2];
			}
		}
	}
	
	return err;
}


function isObject(obj) {
	return Object.prototype.toString.call(obj) === '[object Object]';
}


class Rs extends Transform {
	constructor(reqParams) {
		super();
		
		const me = this;
		
		me.ws = request.post(reqParams);
		
		me.isPiped = false;
		
		// Без этого обработчика и вызова read Transform не отрабатывает до конца
		// https://nodejs.org/api/stream.html#stream_implementing_a_transform_stream
		// Writing data while the stream is not draining is particularly problematic for a Transform,
		// because the Transform streams are paused by default until they are piped or
		// an 'data' or 'readable' event handler is added.
		me.on('readable', function () {
			let data = me.read();
		});
		
		me.pipe(me.ws);
		
		me.on('pipe', function () {
			me.isPiped = true;
		});
	}
	
	_transform(chunk, encoding, cb) {
		cb(null, chunk);
	}
	
	writeRow(data) {
		let row = '';
		
		if (Array.isArray(data)) {
			row = ClickHouse.mapRowAsArray(data);
		} else if (isObject(data)) {
			throw new Error('Error: Inserted data must be an array, not an object.');
		}
		
		let isOk = this.write(
			row + '\n'
		);
		
		this.rowCount++;
		
		if (isOk) {
			return Promise.resolve();
		} else {
			return new Promise((resolve, reject) => {
				this.ws.once('drain', err => {
					if (err) return reject(err);
					
					resolve();
				})
			});
		}
	}
	
	
	exec() {
		let me = this;
		
		return new Promise((resolve, reject) => {
			me.ws
				.on('error', function(err) {
					reject(err);
				})
				.on('response', function (res) {
					if (res.statusCode === 200) {
						return resolve({ r: 1 });
					}
					
					let body = '';
					
					res
						.on('data', data => body += data)
						.on('end', () => {
							res.body = body;
							
							return reject(
								getErrorObj(res)
							);
						});
				});
			
			if ( ! me.isPiped) {
				me.end();
			}
		});
	}
}


class QueryCursor {
	constructor(connection, query, data, opts = {}) {
		this.connection = connection;
		
		this.query = query;
		this.data = data;
		
		this.opts = _.merge({}, opts,  { format: this.connection.opts.format });
		
		// Sometime needs to override format by query
		const formatFromQuery = ClickHouse.getFormatFromQuery(this.query);
		if (formatFromQuery && formatFromQuery !== this.format) {
			this.opts.format = formatFromQuery;
		}
		
		this.useTotals = false;
		this._request = null;
		this.queryId = opts.queryId || uuidv4();
		
		if (this.isDebug) {
			console.log('QueryCursor', {query: this.query, data: this.data, opts: this.opts});
		}
	}
	
	get isInsert() {
		return !!this.query.match(/^insert/i);
	}
	
	get isDebug() {
		return this.connection.opts.debug;
	}
	
	get format() {
		return this.opts.format;
	}
	
	// TODO Add check for white list of formats
	set format(newFormat) {
		this.opts.format = newFormat;
	}
	
	_getBodyForInsert() {
		const me = this;
		
		let query = me.query;
		let data = me.data;
		
		let values          = [],
			fieldList       = [],
			isFirstElObject = false;
		
		if (Array.isArray(data) && Array.isArray(data[0])) {
			values = data;
		} else if (Array.isArray(data) && isObject(data[0])) {
			values = data;
			isFirstElObject = true;
		} else if (isObject(data)) {
			values = [data];
			isFirstElObject = true;
		} else {
			throw new Error('ClickHouse._getBodyForInsert: data is invalid format');
		}
		
		if (isFirstElObject) {
			let m = query.match(/INSERT INTO (.+?) \((.+?)\)/);
			if (m) {
				fieldList = m[2].split(',').map(s => s.trim());
			} else {
				throw new Error('insert query wasnt parsed field list after TABLE_NAME');
			}
		}
		
		return values.map(row => {
			if (isFirstElObject) {
				return ClickHouse.mapRowAsObject(fieldList, row);
			} else {
				return ClickHouse.mapRowAsArray(row);
			}
		}).join('\n');
	}
	
	
	_getReqParams() {
		const me = this;
		
		const {
			reqParams,
			config,
			username,
			password,
			database,
		} = me.connection.opts;
		
		const params = _.merge({
			headers: {
				'Content-Type': 'text/plain'
			},
		}, reqParams);
		
		const configQS = _.merge({}, config, {
			query_id: me.queryId,
		});
		
		if (me.connection.opts.isSessionPerQuery) {
			configQS.session_id = uuidv4();
		}
		
		if (database) {
			configQS.database = database;
		}
		
		const url = new URL(me.connection.url);
		
		if (username) {
			url.searchParams.append('user', username);
		}
		
		if (password) {
			url.searchParams.append('password', password);
		}
		
		Object.keys(configQS).forEach(k => {
			url.searchParams.append(k, configQS[k]);
		});
		
		
		let data = me.data;
		let query = me.query;
		
		if (typeof query === 'string') {
			if (/with totals/i.test(query)) {
				me.useTotals = true;
			}
			
			// Hack for Sequelize ORM
			query = query.trim().trimEnd().replace(/;$/gm, "");
			
			if (query.match(/^(select|show|exists)/i)) {
				if ( ! R_FORMAT_PARSER.test(query)) {
					query += ` FORMAT ${ClickHouse.getFullFormatName(me.format)}`;
				}
				
				query += ';';
				
				if (data && data.external) {
					params['formData'] = data.external.reduce(
						function(formData, external) {
							url.searchParams.append(
								`${external.name}_structure`,
								external.structure || 'str String'
							);
							
							formData[external.name] = {
								value: external.data.join('\n'),
								options: {
									filename: external.name,
									contentType: 'text/plain'
								}
							};
							
							return formData;
						},
						{}
					);
				}
			} else if (me.isInsert) {
				if (query.match(/values/i)) {
					//
				} else {
					query += ' FORMAT TabSeparated';
					
					if (data) {
						params['body'] = me._getBodyForInsert();
					}
				}
			}
		}
		
		if (me.opts.sessionId !== undefined && typeof me.opts.sessionId === 'string') {
			url.searchParams.append('session_id', me.opts.sessionId);
		}
		
		url.searchParams.append('query', query);
		
		if (me.connection.isUseGzip) {
			params.headers['Accept-Encoding']  = 'gzip';
		}
		
		params['url'] = url.toString();
		
		if (me.isDebug) {
			console.log('QueryCursor._getReqParams: params', me.query, params);
		}
		
		return params;
	}
	
	exec(cb) {
		const me = this;
		const reqParams = me._getReqParams();
		
		me._request = request.post(reqParams, (err, res) => {
			if (me.isDebug) {
				console.log('QueryCursor.exec: result', me.query, err, _.pick(res, [
					'statusCode',
					'body',
					'statusMessage',
					'headers'
				]));
			}
			
			if (err) {
				return cb(err);
			} else if (res.statusCode !== 200) {
				return cb(
					getErrorObj(res)
				);
			}
			
			if ( ! res.body) {
				return cb(null, {r: 1});
			}

			try {
				const data = me.getBodyParser()(res.body);
				
				if (me.format === 'json') {
					if (me.useTotals) {
						return cb(null, data);
					}
					
					return cb(null, data.data);
				}
				
				if (me.useTotals) {
					return cb(null, {
						meta: {},
						data,
						totals: {},
						rows: {},
						statistics: {},
					});
				}
				
				return cb(null, data);
			} catch (err) {
				cb(err);
			}
		});
	}

	getBodyParser() {
		if (this.format === 'json') {
			return JSON.parse;
		}
		
		if (this.format === 'tsv') {
			return parseTSV;
		}
		
		if (this.format === 'csv') {
			return parseCSV;
		}
		
		throw new Error(`CursorQuery.getBodyParser: unknown format "${this.format}"`);
	};
	
	getStreamParser() {
		if (this.format === 'json') {
			return parseJSONStream;
		}
		
		if (this.format === 'tsv') {
			return parseTSVStream;
		}
		
		if (this.format === 'csv') {
			return parseCSVStream;
		}
		
		throw new Error(`CursorQuery.getStreamParser: unknown format "${this.format}"`);
	}

	withTotals() {
		this.useTotals  = true;
		
		return this;
	}
	
	toPromise() {
		let me = this;
		
		return new Promise((resolve, reject) => {
			me.exec(function (err, data) {
				if (err) return reject(err);
				
				resolve(data);
			})
		});
	}
	
	stream() {
		const me = this;
		
		const reqParams = me._getReqParams();
		
		if (me.isInsert) {
			const rs = new Rs(reqParams);
			rs.query = me.query;
			
			me._request = rs;
			
			return rs;
		} else {
			const streamParser = this.getStreamParser()();
			
			const rs = new Readable({ objectMode: true });
			rs._read = () => {};
			rs.query = me.query;
			
			const tf = new Transform({ objectMode: true });
			let isFirstChunk = true;
			tf._transform = function (chunk, encoding, cb) {
				
				// В независимости от формата, в случае ошибки, в теле ответа будет текс,
				// подпадающий под регулярку R_ERROR.
				if (isFirstChunk) {
					isFirstChunk = false;
					
					if (R_ERROR.test(chunk.toString())) {
						streamParser.emit('error', new Error(chunk.toString()));
						rs.emit('close');
						
						return cb();
					}
				}
				
				cb(null, chunk);
			};
			
			let metaData = {};
			
			const requestStream = request.post(reqParams);
			
			// Не делаем .pipe(rs) потому что rs - Readable,
			// а для pipe нужен Writable
			let s;
			if (me.connection.isUseGzip) {
				const z = zlib.createGunzip();
				s = requestStream.pipe(z).pipe(tf).pipe(streamParser)
			} else {
				s = requestStream.pipe(tf).pipe(streamParser)
			}
			
			s
				.on('error', function (err) {
					rs.emit('error', err);
				})
				.on('header', header => {
					metaData = _.merge({}, header);
				})
				.on('footer', footer => {
					rs.emit('meta', _.merge(metaData, footer));
				})
				.on('data', function (data) {
					rs.emit('data', data);
				})
				.on('close', function () {
					rs.emit('close');
				})
				.on('end', function () {
					rs.emit('end');
				});
			
			rs.__pause = rs.pause;
			rs.pause  = () => {
				rs.__pause();
				requestStream.pause();
				streamParser.pause();
			};
			
			rs.__resume = rs.resume;
			rs.resume = () => {
				rs.__resume();
				requestStream.resume();
				streamParser.resume();
			};
			
			me._request = rs;
			
			return stream2asynciter(rs);
		}
	}
	
	
	destroy() {
		const me = this;
		
		let isCallDestroy = false;
		
		if (me._request instanceof Readable) {
			isCallDestroy = true;
			me._request.destroy();
		} else if (me._request) {
			isCallDestroy = true;
			me._request.abort();
		}
		
		// To trying to kill query by query id
		if (me.queryId) {
			
			// Because this realesation work with session witout any ideas,
			// we need use this hack
			me.connection.query(
				`KILL QUERY WHERE query_id = '${me.queryId}' SYNC`, {}, {
					sessionId: uuidv4(),
				}
			).exec(() => {});
		}
		
		if (isCallDestroy) {
			return ;
		}
		
		throw new Error('QueryCursor.destroy error: private field _request is invalid');
	}
}


class ClickHouse {
	constructor(opts = {}) {
		this.opts = _.merge(
			{
				debug: false,
				database: DATABASE,
				password: '',
				basicAuth: null,
				isUseGzip: false,
				config: {
					session_timeout                         : 60,
					output_format_json_quote_64bit_integers : 0,
					enable_http_compression                 : 0
				},
                format: 'json',
				isSessionPerQuery: false,
			},
			opts
		);
		
		
		let url  = opts.url || opts.host || URI,
			port = opts.port || PORT;
		
		if ( ! url.match(/^https?/)) {
			url = 'http://' + url;
		}
		
		const u = new URL(url);
		
		if (u.protocol === 'https:' && (port === 443 || port === 8123)) {
			u.port = '';
		} else if (port) {
			u.port = port;
		}
		
		this.opts.url = u.toString();

		this.opts.username = this.opts.user || this.opts.username || USERNAME;
		
		
		if (this.opts.config) {
			const { database } = this.opts.config;
			
			if (database && database !== this.opts.database) {
				this.opts.database = database;
			}
		}
	}
	
	get sessionId() {
		return this.opts.config.session_id;
	}
	
	set sessionId(sessionId) {
		this.opts.config.session_id = '' + sessionId;
		return this;
	}
	
	noSession() {
		delete this.opts.config.session_id;
		
		return this;
	}
	
	get sessionPerQuery() {
		return this.opts.isSessionPerQuery;
	}
	
	setSessionPerQuery(value) {
		this.opts.isSessionPerQuery = !!value;
		
		return this;
	}
	
	get sessionTimeout() {
		return this.opts.config.session_timeout;
	}
	
	set sessionTimeout(timeout) {
		this.opts.config.session_timeout = timeout;
		return this;
	}
	
	get url() {
		if (this.opts.basicAuth) {
			const u = new URL(this.opts.url);

			u.username = this.opts.basicAuth.username || '';
			u.password = this.opts.basicAuth.password || '';

			return u.toString();
		}

		return this.opts.url;
	}
	
	set url(url) {
		this.opts.url = url;
		return this;
	}
	
	get port() {
		return this.opts.port;
	}
	
	set port(port) {
		this.opts.port = port;
		return this;
	}
	
	get isUseGzip() {
		return this.opts.isUseGzip;
	}
	
	set isUseGzip(val) {
		this.opts.isUseGzip = !!val;
		
		this.opts.config.enable_http_compression = this.opts.isUseGzip ? 1 : 0;
	}
	
	static mapRowAsArray(row) {
		return row
			.map(value => encodeValue(false, value, 'TabSeparated'))
			.join('\t');
	}
	
	static mapRowAsObject(fieldList, row) {
		return fieldList
			.map(f => {
				return encodeValue(false, row[f] != null ? row[f] : '', 'TabSeparated');
			})
			.join('\t');
	}
	
	static getFullFormatName(format = '') {
		if ( ! FORMATS[format]) {
			throw new Error(`Clickhouse.getFullFormatName: unknown format "${format}`);
		}
		
		return FORMATS[format];
	}
	
	static getFormatFromQuery(query = '') {
		if ( ! query) {
			throw new Error(`Clickhouse.getFormatFromQuery: query is empty!`);
		}
		
		// We use regexp with "g" flag then match doen't return first group.
		// So, use exec.
		const m = R_FORMAT_PARSER.exec(query);
		if (m) {
			const format = m[1];
			if ( ! REVERSE_FORMATS[format]) {
				throw new Error(`Clickhouse.getFormatFromQuery: unknown format "${format}"!`);
			}
			
			return REVERSE_FORMATS[format];
		}
		
		return '';
	}
	
	static getFormats() {
		return Object.keys(FORMATS).map(k => ({ format: k, fullFormatExpr: FORMATS[k], }));
	}
	
	query(...args) {
		if (typeof args[args.length - 1] === 'function') {
			const newArgs = args.slice(0, args.length);
			const cb = args[args.length - 1];
			
			return new QueryCursor(this, ...newArgs).exec(cb);
		}
		
		return new QueryCursor(this, ...args);
	}
	
	insert(query, data) {
		return new QueryCursor(this, query, data);
	}
}

module.exports = {
    ClickHouse,
};

