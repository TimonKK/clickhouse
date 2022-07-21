/// <reference types="node" />

declare module 'clickhouse' {
  import {Stream} from 'stream';

  type callbackExec = (error: Error, rows?: Object[]) => void;

  export type ClickHouseOptions = {
    url?: string,
    host?: string,
    port?: number,
    database?: string,
    debug?: false,
    basicAuth?: {
      username: string,
      password: string,
    },
    isUseGzip?: boolean,
    trimQuery?: boolean,
    usePost?: boolean,
    format?: "json" | "csv" |"tsv",
    raw?: boolean,
    config?: {
      session_id?: string;
      session_timeout?: number,
      output_format_json_quote_64bit_integers?: number;
      enable_http_compression?: number;
      database?: string;
    },
  }

  export class ClickHouse {
    constructor(opts: ClickHouseOptions);
    query(query: String, reqParams?: object): QueryCursor;
    insert(query: String, data?: object): QueryCursor;
    sessionId: string;
  }

  export class WriteStream extends Stream.Transform {
    writeRow(data: Array<any> | string): Promise<void>;
    exec(): Promise<{}>;
  }

  class QueryCursor {
    toPromise(): Promise<Object[]>;
    exec(callback: callbackExec): void;
    stream(): Stream & WriteStream;
  }
}
