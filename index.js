'use strict';

const zlib = require('zlib');

const
	_                = require('lodash'),
	request          = require('request'),
	stream           = require('stream'),
	querystring      = require('querystring'),
	JSONStream       = require('JSONStream'),
	through       	 = require('through'),
	stream2asynciter = require('stream2asynciter'),
	{ URL }          = require('url'),
	tsv              = require('tsv');


/**
 * Content-Encoding: gzip
 * Accept-Encoding: gzip
 * и включить настройку ClickHouse enable_http_compression.
 *
 * session_id
 *
 * session_timeout
 */

var SEPARATORS = {
	TSV: "\t",
	CSV: ",",
	Values: ","
};

var ALIASES = {
	TabSeparated: "TSV"
};

var ESCAPE_STRING = {
	TSV: function (v, quote) {return v.replace (/\\/g, '\\\\').replace (/\\/g, '\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n')},
	CSV: function (v, quote) {return v.replace (/\"/g, '""')},
};

var ESCAPE_NULL = {
	TSV: "\\N",
	CSV: "\\N",
	Values: "\\N",
	// JSONEachRow: "\\N",
};

const R_ERROR = new RegExp('Code: ([0-9]{2}), .*Exception:');

const URI = 'localhost';

const PORT = 8123;

const DATABASE = 'default';
const USERNAME = 'default';

function parseCSV(body, options = { header: true }) {
	const data = new tsv.Parser(SEPARATORS.CSV, options).parse(body);
	data.splice(data.length - 1, 1);
	return data;
}

function parseJSON(body) {
	return JSON.parse(body);
}

function parseTSV(body, options = { header: true }) {
	const data = new tsv.Parser(SEPARATORS.TSV, options).parse(body);
	data.splice(data.length - 1, 1);
	return data;
}

function parseCSVStream(s) {
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

function parseTSVStream(s) {
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

function encodeValue(quote, v, format, isArray) {
	format = ALIASES[format] || format;
	
	switch (typeof v) {
		case 'string':
			if (isArray) {
				return `'${ESCAPE_STRING[format] ? ESCAPE_STRING[format](v, quote) : v}'`;
			} else {
				return ESCAPE_STRING[format] ? ESCAPE_STRING[format](v, quote) : v;
			}
		case 'number':
			if (isNaN (v))
				return 'nan';
			if (v === +Infinity)
				return '+inf';
			if (v === -Infinity)
				return '-inf';
			if (v === Infinity)
				return 'inf';
			return v;
		case 'object':
			
			// clickhouse allows to use unix timestamp in seconds
			if (v instanceof Date) {
				return ("" + v.valueOf()).substr (0, 10);
			}
			
			// you can add array items
			if (v instanceof Array) {
				// return '[' + v.map(encodeValue.bind(this, true, format)).join (',') + ']';
				return '[' + v.map(function (i) {
					return encodeValue(true, i, format, true);
				}).join(',') + ']';
			}
			
			// TODO: tuples support
			if (!format) {
				console.trace ();
			}
			
			if (v === null) {
				return format in ESCAPE_NULL ? ESCAPE_NULL[format] : v;
			}
			
			return format in ESCAPE_NULL ? ESCAPE_NULL[format] : v;
		case 'boolean':
			return v === true ? 1 : 0;
	}
}

function getErrorObj(res) {
	let err = new Error(`${res.statusCode}: ${res.body || res.statusMessage}`);
	
	if (res.body) {
		let m = res.body.match(R_ERROR);
		if (m) {
			if (m[1] && isNaN(parseInt(m[1])) === false) {
				err.code    = parseInt(m[1]);
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


class Rs extends stream.Transform {
	constructor(reqParams) {
		super();
		
		let me = this;
		
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
			throw new Error('Sorry, but it is not work!');
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
					
					return reject(
						getErrorObj(res)
					)
				});
			
			if ( ! me.isPiped) {
				me.end();
			}
		});
	}
}


class QueryCursor {
	constructor(query, reqParams, opts) {
		this.isInsert  = !!query.match(/^insert/i);
		this.fieldList = null;
		this.query     = query;
		this.reqParams = _.merge({}, reqParams);
		this.opts      = opts;
		this.useTotals = false;
		this._request  = null;
	}
	
	
	exec(cb) {
		let me = this;
		
		if (me.opts.debug) {
			console.log('exec req headers', me.reqParams.headers);
		}
		
		me._request = request.post(me.reqParams, (err, res) => {
			if (me.opts.debug) {
				console.log('exec', err, _.pick(res, [
					'statusCode',
					'body',
					'statusMessage'
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
			
			if (me.opts.debug) {
				console.log('exec res headers', res.headers);
			}

			try {
				const result = this._parseRowsByFormat(res.body);
				cb(null, me.useTotals ? result : result.data || result)
			} catch (e) {
				cb(e);
			}
		});
	}

	_parseRowsByFormat(body, isStream = false) {
		let result = null;
		let ws;
        switch (this._getFormat()) {
			case "json":
				result = !isStream && parseJSON(body) || parseJSONStream();
				break;
			case "tsv":
				result = !isStream && parseTSV(body) || parseTSVStream(new Set());
				break;
			case "csv":
				result = !isStream && parseCSV(body) || parseCSVStream(new Set());
				break;
			default:
				result = body;
		}
		return result;
	};

    _getFormat() {
        return this.opts.sessionFormat || this.opts.format;
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
		const
			me      = this,
			isDebug = me.opts.debug;
		
		if (isDebug) {
			console.log('stream req headers', me.reqParams.headers);
		}
		
		if (me.isInsert) {
			const rs = new Rs(this.reqParams);
			rs.query = this.query;
			
			me._request = rs;
			
			return rs;
		} else {
			const streamParser = this._parseRowsByFormat(null, true);
			
			const rs = new stream.Readable({ 	objectMode: true });
			rs._read = () => {};
			rs.query = this.query;
			
			const tf = new stream.Transform({ objectMode: true });
			let isFirstChunck = true;
			tf._transform = function (chunk, encoding, cb) {
				// Если для первого chuck первый символ блока данных не '{', тогда:
				// 1. в теле ответа не JSON
				// 2. сервер нашел ошибку в данных запроса
				if (isFirstChunck &&  (
                    (me._getFormat() === "json" && chunk[0] !== 123) &&
                    (me._getFormat() === "csv" && chunk[0] !== 110) &&
                    (me._getFormat() === "tsv" && chunk[0] !== 110)
                )) {
					this.error = new Error(chunk.toString());
					
					streamParser.emit("error", this.error);
					rs.emit('close');
					
					return cb();
				}
				
				isFirstChunck = false;
				
				cb(null, chunk);
			};
			
			let metaData = {};
			
			const requestStream = request.post(this.reqParams);
			
			// Не делаем .pipe(rs) потому что rs - Readable,
			// а для pipe нужен Writable
			let s = null;
			if (me.opts.isUseGzip) {
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
		if (this._request instanceof stream.Readable) {
			return this._request.destroy();
		}
		
		if (this._request) {
			return this._request.abort();
		}
		
		throw new Error('QueryCursor.destroy error: private field _request is invalid');
	}
}


class ClickHouse {
	constructor(opts) {
		if ( ! opts) {
			opts = {};
		}
		
		this.opts = _.extend(
			{
				debug: false,
				database: DATABASE,
				password: '',
				basicAuth: null,
				isUseGzip: false,
				config: {
					// session_id                              : Date.now(),
					session_timeout                         : 60,
					output_format_json_quote_64bit_integers : 0,
					enable_http_compression                 : 0
				},
                format: "json", // "json" || "csv" || "tsv"
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
	
	
	escape(str) {
		return str.replace(/\t|\n/g, '\\t');
	}
	
	
	static mapRowAsArray(row) {
		return row.map(function(value) {
			return encodeValue(false, value, 'TabSeparated');
		}).join('\t');
	}

	_getFormat(query) {
		let format = "";
		switch (this.opts.format) {
		case "json":
			format = this._parseFormat(query, " format JSON");
			break;
		case "tsv":
            format = this._parseFormat(query, " format TabSeparatedWithNames");
			break;
		case "csv":
			format = this._parseFormat(query, " format CSVWithNames");
			break;
		default:
			format = " ";
		}
		return format;
	};

    _parseFormat(query, def) {
        if (query.match(/format/mg) === null) {
            this.opts.sessionFormat = this.opts.format;
            return def;
        }
        if (query.match(/format JSON/mg) !== null) {
            this.opts.sessionFormat = "json";
        } else if (query.match(/format TabSeparated/mg) !== null) {
            this.opts.sessionFormat = "tsv";
        } else if (query.match(/format CSV/mg) !== null) {
            this.opts.sessionFormat = "csv";
        }
        return "";
    }

	_mapRowAsObject(fieldList, row) {
		return fieldList.map(f => encodeValue(false, row[f] != null ? row[f] : '', 'TabSeparated')).join('\t');
	}
	
	
	_getBodyForInsert(query, data) {
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
				return this._mapRowAsObject(fieldList, row);
			} else {
				return ClickHouse.mapRowAsArray(row);
			}
		}).join('\n');
	}
	
	
	_getReqParams(query, data) {
		let me = this;
		
		let reqParams = _.merge({}, me.opts.reqParams),
			configQS = _.merge({}, me.opts.config);
		
		if (me.opts.database) {
			configQS.database = me.opts.database;
		}
		
		if (typeof query === 'string') {
			let sql = query.trim();
			
			// Hack for Sequelize ORM
			sql = sql.trimEnd().replace(/;$/gm, "");

			if (sql.match(/^(select|show|exists)/i)) {
            	reqParams['url']  = me.url + '?query=' + encodeURIComponent(sql + this._getFormat(sql) + ';') + '&' + querystring.stringify(configQS);
				if (me.opts.username) {
					reqParams['url'] = reqParams['url'] + '&user=' + me.opts.username;
				}

				if (this.opts.password) {
					reqParams['url'] = reqParams['url'] + '&password=' + me.opts.password;
				}
				
				if (data && data.external) {
					let formData = {};
					
					for (let external of data.external) {
						reqParams.url += `&${external.name}_structure=${external.structure || 'str String'}`;
						
						formData[external.name] = {
							value: external.data.join('\n'),
							options: {
								filename: external.name,
								contentType: 'text/plain'
							}
						}
					}
					
					reqParams['formData'] = formData;
				}
			} else if (query.match(/^insert/i)) {
				reqParams['url']  = me.url + '?query=' + encodeURIComponent(sql + ' FORMAT TabSeparated') + '&' + querystring.stringify(configQS);
				
				if (me.opts.username) {
					reqParams['url'] = reqParams['url'] + '&user=' + me.opts.username;
				}
				
				if (this.opts.password) {
					reqParams['url'] = reqParams['url'] + '&password=' + me.opts.password;
				}
				
				if (data) {
					reqParams['body'] = me._getBodyForInsert(sql, data);
				}
			} else {
				reqParams['url']  = me.url + '?query=' + encodeURIComponent(sql + ";") + '&' + querystring.stringify(configQS);
				
				if (me.opts.username) {
					reqParams['url'] = reqParams['url'] + '&user=' + me.opts.username;
				}
				
				if (this.opts.password) {
					reqParams['url'] = reqParams['url'] + '&password=' + me.opts.password;
				}
			}
			
			reqParams['headers'] = {
				'Content-Type': 'text/plain'
			}
		}
		
		if (me.opts.isUseGzip) {
			//reqParams.headers['Content-Encoding'] = 'gzip';
			reqParams.headers['Accept-Encoding']  = 'gzip';
			// reqParams['gzip'] = true;
		}
		
		if (me.opts.debug) {
			console.log('DEBUG', reqParams);
		}
		
		return reqParams;
	}
	
	
	query(...args) {
		if (args.length === 2 && typeof args[args.length - 1] === 'function') {
			return new QueryCursor(args[0], this._getReqParams(args[0], null), this.opts).exec(args[args.length - 1]);
		} else {
			return new QueryCursor(args[0], this._getReqParams(args[0], args[1]), this.opts);
		}
	}
	
	
	insert(query, data) {
		return new QueryCursor(query, this._getReqParams(query, data), this.opts);
	}
}

module.exports = {
    ClickHouse
};

