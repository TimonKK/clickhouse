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

  export class WriteStream extends stream.Transform {
    writeRow(data: Array<any>);
    exec();
  }

  class QueryCursor {
    async toPromise(): Promise<Object[]>;
    exec(callback: callbackExec);
    stream(): Stream | WriteStream;
  }
}
