/// <reference types="node" />

declare module 'clickhouse' {
  import { Transform, Stream } from 'stream';

  type callbackExec = (error: Error, rows?: Object[]) => void;

  export interface DriverOptions {
    debug: boolean,
    database: string,
    password: string,
    basicAuth: {
      username: string
      password: string
    },
    isUseGzip: boolean,
    config: {
      session_timeout: number,
      output_format_json_quote_64bit_integers: number,
      enable_http_compression: number
    },
    format: 'json' | 'tsv' | 'csv', 
    raw: boolean,
    isSessionPerQuery: boolean,
    trimQuery: boolean,
    usePost: boolean,
  }

  export class ClickHouse {
    constructor(opts: DriverOptions);
    query(query: String, reqParams?: object): QueryCursor;
    insert(query: String, data?: object): QueryCursor;
    sessionId: string;
  }

  export class WriteStream extends Transform {
    writeRow(data: Array<any> | string): Promise<void>;
    exec(): Promise<{}>;
  }

  class QueryCursor {
    toPromise(): Promise<Object[]>;
    exec(callback: callbackExec): void;
    stream(): Stream & WriteStream;
  }
}
