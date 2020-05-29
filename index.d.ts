/// <reference types="node" />

declare module 'clickhouse' {
  import {Stream} from 'stream';

  type callbackExec = (error: Error, rows?: Object[]) => void;

  export class ClickHouse {
    constructor(opts: Object);
    query(query: String, reqParams?: object): QueryCursor;
    insert(query: String, data?: object): QueryCursor;
    sessionId: string;
  }

  export class WriteStream extends Stream.Transform {
    writeRow(data: Array<any>): Promise<void>;
    exec(): Promise<{}>;
  }

  class QueryCursor {
    toPromise(): Promise<Object[]>;
    exec(callback: callbackExec): void;
    stream(): Stream & WriteStream;
  }
}
