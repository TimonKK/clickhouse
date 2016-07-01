'use strict';

var _           = require('lodash'),
	request     = require('request'),
	stream      = require('stream'),
	querystring = require('querystring');


var TypeList = ['UInt8', 'UInt16', 'UInt32', 'UInt64', 'Int8', 'Int16', 'Int32', 'Int64'];

class TypeCast {
	constructor() {
		this.castMap = TypeList.reduce(
			(obj, type) => {
				obj[type] = value => { return parseInt(value, 10) };
				return obj;
			},
			{
				'Date'   : value =>value,
				'String' : value =>value
			}
		);
	}
	
	
	cast(type, value) {
		return this.castMap[type] ? this.castMap[type](value) : value;
	}
}


class ClickHouse {
	constructor(_opts) {
		this.opts = _.extend(
			{
				url   : 'http://localhost',
				port  : 8123,
				debug : false
			},
			_opts
		);
		
		this.typeCast = new TypeCast();
	}
	
	getHost() {
		return this.opts.url + ':' + this.opts.port;
	}
	
	/**
	 * Get url query
	 * @param {String} query
	 * @returns {String}
	 */
	getUrl(query) {
		var params = {};
		
		if (query) params['query'] = query + ' FORMAT TabSeparatedWithNamesAndTypes';
		
		if (Object.keys(params).length == 0) return new Error('query is empty');
		
		return this.getHost() + '?' + querystring.stringify(params);
	}
	
	
	/**
	 * Parse data
	 * @param {Buffer} data
	 * @returns {Array}
	 */
	_parseData(data) {
		var me         = this,
			rows       = data.toString('utf8').split('\n'),
			columnList = rows[0].split('\t'),
			typeList   = rows[1].split('\t');
		
		// Удаляем строки с заголовками и типами столбцов И завершающую строку
		rows = rows.slice(2, rows.length - 1);
		
		columnList = columnList.reduce(
			function (arr, column, i) {
				arr.push({
					name : column,
					type : typeList[i]
				});
				
				return arr;
			},
			[]
		);
		
		return rows.map(function (row, i) {
			let columns = row.split('\t');
			
			return columnList.reduce(
				function (obj, column, i) {
					obj[column.name] = me.typeCast.cast(column.type, columns[i]);
					return obj;
				},
				{}
			);
		});
	}
	
	
	/**
	 * Exec query
	 * @param {String} query
	 * @param {Function} cb
	 * @returns {Stream|undefined}
	 */
	query(query, cb) {
		var me  = this,
			url = me.getUrl(query);
		
		if (me.opts.debug) console.log('url', url);
		
		if (cb) {
			return request.get(url, function (error, response, body) {
				if ( ! error && response.statusCode == 200) {
					cb(null, me._parseData(body));
				} else {
					cb(error);
				}
			});
		} else {
			let rs = new stream.Readable();
			rs._read = function (chunk) {
				if (me.opts.debug) console.log('rs _read chunk', chunk);
			};
			
			let queryStream = request.get(url);
			queryStream.columnsName = null;
			
			let responseStatus = 200;
			
			queryStream
				.on('response', function (response) {
					responseStatus = response.statusCode;
				})
				.on('error', function (err) {
					rs.emit('error', err);
				})
				.on('data', function (data) {
					
					// Если ошибка на строне сервера (не правильный запрос, ещё что-то),
					// то придёт один пакет данных с сообщением об ошибке
					if (responseStatus != 200) return rs.emit('error',  data.toString('utf8'));
					
					var rows = data.toString('utf8').split('\n');
					
					if ( ! queryStream.columnList) {
						let columnList = rows[0].split('\t');
						let typeList   = rows[1].split('\t');
						
						// Удаляем строки с заголовками и типами столбцов И завершающую строку
						rows = rows.slice(2, rows.length - 1);
						
						queryStream.columnList = columnList.reduce(
							function (arr, column, i) {
								arr.push({
									name : column,
									type : typeList[i]
								});
								
								return arr;
							},
							[]
						);
						
						if (me.opts.debug) console.log('columns', queryStream.columnList);
					}
					
					if (me.opts.debug) console.log('raw data', data.toString('utf8'));
					
					for(let i=0; i<rows.length; i++) {
						let columns = rows[i].split('\t');
						rs.emit(
							'data',
							queryStream.columnList.reduce(
								(o, c, i) => {
									o[c.name] = me.typeCast.cast(c.type, columns[i]);
									return o;
								},
								{}
							)
						);
					}
				})
				.on('end', function () {
					rs.emit('end');
				});
			
			return rs;
		}
	}
	

	/**
	 * Insert rows by one query
	 * @param {String} tableName
	 * @param {Array} values List or values. Each value is array of columns
	 * @param {Function} cb
	 * @returns
	 */
	insertMany(tableName, values, cb) {
		var url = `INSERT INTO ${tableName} FORMAT TabSeparated`;
		
		request.post(
			{
				url     : this.getHost() + '?query=' + url,
				body    : values
					.map(i => i.join('\t'))
					.join('\n'),
				headers : {
					'Content-Type': 'text/plain'
				}
			},
			function (error, response, body) {
				if ( ! error && response.statusCode == 200) {
					cb(null, body);
				} else {
					return cb(error || body);
				}
			}
		);
	}
}


module.exports = ClickHouse;