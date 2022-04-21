/// <reference types="node" />

declare module 'clickhouse' {
  import {Stream} from 'stream';

  type callbackExec = (error: Error, rows?: Object[]) => void;

  export class ClickHouse {
    constructor(opts: Object);
    query<R extends Object[] = Object[]>(query: String, reqParams?: object): QueryCursor<R>;
    insert<R extends Object[] = Object[]>(query: String, data?: object): QueryCursor<R>;
    sessionId: string;
  }

  export class WriteStream extends Stream.Transform {
    writeRow(data: Array<any> | string): Promise<void>;
    exec(): Promise<{}>;
  }

  class QueryCursor<R extends Object[] = Object[]> {
    toPromise(): Promise<R>;
    exec(callback: callbackExec): void;
    stream(): Stream & WriteStream;
  }
}
