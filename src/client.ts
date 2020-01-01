import * as WS from "socket.io-client";
import {
  default as EventEmitterType,
  EventEmitter,
  ListenerFn
} from "eventemitter3";
import isString from "./utils/is-string";
import isObject from "./utils/is-object";
import { ExecutionResultDataDefault } from "graphql/execution/execute";
import { print } from "graphql/language/printer";
import { DocumentNode, ASTNode } from "graphql/language/ast";
import { getOperationAST } from "graphql/utilities/getOperationAST";
import $$observable from "symbol-observable";

import MessageTypes from "./message-types";

export interface ExecutionResult<TData = ExecutionResultDataDefault> {
  errors?: ReadonlyArray<Error>;
  data?: TData | null;
}

export interface Observer<T> {
  next?: (value: T) => void;
  error?: (error: Error) => void;
  complete?: () => void;
}

export interface IncomingMessage {
  id: string;
  type: string;
  payload: ExecutionResult;
}

export type OutgoingMessagePayload = { query: string | ASTNode } | object;

export interface OperationHandler {
  (errors: Error[], result: null): void;
  (errors: null, result: ExecutionResult | null): void;
}

type OperationError =
  | { errors?: readonly FormatedError[] }
  | readonly FormatedError[]
  | FormatedError;

export interface Observable<T> {
  subscribe(
    observer: Observer<T>
  ): {
    unsubscribe: () => void;
  };
}

interface ConnectionCallback {
  (error: Error[] | null, result: unknown): void;
}

export interface OperationOptions {
  query: string | DocumentNode;
  variables?: Object;
  operationName?: string;
  [key: string]: unknown;
}

export interface FormatedError extends Error {
  originalError?: unknown;
}

export interface Operation {
  options: OperationOptions;
  handler: OperationHandler;
}

export type Operations = {
  [K in string]?: Operation;
};

export interface Middleware {
  applyMiddleware(options: OperationOptions, next: Function): void;
}

export type ConnectionParams = {
  [paramName: string]: unknown;
};

export type ConnectionParamsOptions =
  | ConnectionParams
  | (() => Promise<ConnectionParams>)
  | Promise<ConnectionParams>;

export interface ClientOptions {
  connectionParams?: ConnectionParamsOptions;
  connectionCallback?: ConnectionCallback;
  lazy?: boolean;
  reconnect?: boolean;
  reconnectionAttempts?: number;
}

export class SubscriptionClient {
  public client: SocketIOClient.Socket | null;
  public operations: Operations;
  private url: string;
  private nextOperationId: number;
  private connectionParams: () => Promise<ConnectionParams>;
  private connectionCallback?: ConnectionCallback;
  private eventEmitter: EventEmitterType;
  private lazy: boolean;
  private middlewares: Middleware[];

  constructor(url: string, options?: ClientOptions) {
    const { connectionCallback, connectionParams = {}, lazy = false } =
      options || {};

    this.connectionCallback = connectionCallback;
    this.url = url;
    this.operations = {};
    this.nextOperationId = 0;
    this.lazy = !!lazy;
    this.eventEmitter = new EventEmitter();
    this.middlewares = [];
    this.client = null;
    this.connectionParams = this.getConnectionParams(connectionParams);

    if (!this.lazy) {
      this.connect();
    }
  }

  // public get status() {
  //   if (this.client === null) {
  //     return this.wsImpl.CLOSED;
  //   }

  //   return this.client.readyState;
  // }

  public request(request: OperationOptions): Observable<ExecutionResult> {
    const observable: Observable<ExecutionResult> = {
      [$$observable]() {
        return observable;
      },
      subscribe: (
        observerOrNext:
          | Observer<ExecutionResult>
          | ((v: ExecutionResult) => void),
        onError?: (error: Error) => void,
        onComplete?: () => void
      ) => {
        const observer = this.getObserver(observerOrNext, onError, onComplete);

        const handler: OperationHandler = (
          error: Error[] | null,
          result: ExecutionResult | null
        ) => {
          if (error === null && result === null) {
            if (observer.complete) {
              observer.complete();
            }
          } else if (error) {
            if (observer.error) {
              observer.error(error[0]);
            }
          } else {
            if (observer.next) {
              observer.next(result!);
            }
          }
        };

        const opId = this.executeOperation(request, handler);

        return {
          unsubscribe: () => {
            this.unsubscribe(opId);
          }
        };
      }
    };
    return observable;
  }

  public on(
    eventName: string,
    callback: ListenerFn,
    context?: unknown
  ): Function {
    const handler = this.eventEmitter.on(eventName, callback, context);

    return () => {
      handler.off(eventName, callback, context);
    };
  }

  public onConnected(callback: ListenerFn, context?: unknown): Function {
    return this.on("connected", callback, context);
  }

  public onConnecting(callback: ListenerFn, context?: unknown): Function {
    return this.on("connecting", callback, context);
  }

  public onDisconnected(callback: ListenerFn, context?: unknown): Function {
    return this.on("disconnected", callback, context);
  }

  public onReconnected(callback: ListenerFn, context?: unknown): Function {
    return this.on("reconnected", callback, context);
  }

  public onReconnecting(callback: ListenerFn, context?: unknown): Function {
    return this.on("reconnecting", callback, context);
  }

  public onError(callback: ListenerFn, context?: unknown): Function {
    return this.on("error", callback, context);
  }

  public unsubscribeAll() {
    Object.keys(this.operations).forEach(subId => {
      this.unsubscribe(subId);
    });
  }

  public applyMiddlewares(
    options: OperationOptions
  ): Promise<OperationOptions> {
    return new Promise((resolve, reject) => {
      const queue = (funcs: Middleware[], scope: unknown) => {
        const next = (error?: unknown) => {
          if (error) {
            reject(error);
          } else {
            if (funcs.length > 0) {
              const f = funcs.shift();
              if (f) {
                f.applyMiddleware.apply(scope, [options, next]);
              }
            } else {
              resolve(options);
            }
          }
        };
        next();
      };

      queue([...this.middlewares], this);
    });
  }

  public use(middlewares: Middleware[]): SubscriptionClient {
    middlewares.map(middleware => {
      if (typeof middleware.applyMiddleware === "function") {
        this.middlewares.push(middleware);
      } else {
        throw new Error(
          "Middleware must implement the applyMiddleware function."
        );
      }
    });

    return this;
  }

  public close() {
    this.client?.close();
    this.client = null;
  }

  private getConnectionParams(
    connectionParams: ConnectionParamsOptions
  ): () => Promise<ConnectionParams> {
    return (): Promise<ConnectionParams> =>
      new Promise((resolve, reject) => {
        if (typeof connectionParams === "function") {
          try {
            return resolve(connectionParams.call(null));
          } catch (error) {
            return reject(error);
          }
        }

        resolve(connectionParams);
      });
  }

  private executeOperation(
    options: OperationOptions,
    handler: OperationHandler
  ): string {
    if (this.client === null) {
      this.connect();
    }

    const opId = this.generateOperationId();
    this.operations[opId] = { options: options, handler };

    this.applyMiddlewares(options)
      .then(processedOptions => {
        this.checkOperationOptions(processedOptions);
        if (this.operations[opId]) {
          this.operations[opId] = { options: processedOptions, handler };
          this.sendMessage(opId, MessageTypes.GQL_START, processedOptions);
        }
      })
      .catch((error: OperationError) => {
        this.unsubscribe(opId);
        handler(this.formatErrors(error), null);
      });

    return opId;
  }

  private getObserver<T>(
    observerOrNext: Observer<T> | ((v: T) => void),
    error?: (e: Error) => void,
    complete?: () => void
  ) {
    if (typeof observerOrNext === "function") {
      return {
        next: (v: T) => observerOrNext(v),
        error: (e: Error) => error && error(e),
        complete: () => complete && complete()
      };
    }

    return observerOrNext;
  }

  private checkOperationOptions(options: OperationOptions) {
    const { query, variables, operationName } = options;

    if (
      (!isString(query) && !getOperationAST(query, operationName)) ||
      (operationName && !isString(operationName)) ||
      (variables && !isObject(variables))
    ) {
      throw new Error(
        "Incorrect option types. query must be a string or a document," +
          "`operationName` must be a string, and `variables` must be an object."
      );
    }
  }

  private buildMessage(
    id: string | undefined,
    type: string,
    payload: OutgoingMessagePayload
  ) {
    const payloadToReturn =
      "query" in payload
        ? {
            ...payload,
            query:
              typeof payload.query === "string"
                ? payload.query
                : print(payload.query)
          }
        : payload;

    return {
      id,
      type,
      payload: payloadToReturn
    };
  }

  // ensure we have an array of errors
  private formatErrors(errors: OperationError): FormatedError[] {
    if (Array.isArray(errors)) {
      return errors;
    }

    // TODO  we should not pass ValidationError to callback in the future.
    // ValidationError
    if ("errors" in errors && errors.errors) {
      return this.formatErrors(errors.errors);
    }

    if ("message" in errors && errors.message) {
      return [errors];
    }

    return [
      {
        name: "FormatedError",
        message: "Unknown error",
        originalError: errors
      }
    ];
  }

  private sendMessage(
    id: string | undefined,
    type: string,
    payload: OutgoingMessagePayload
  ) {
    this.sendMessageRaw(this.buildMessage(id, type, payload));
  }

  private sendMessageRaw(message: unknown) {
    this.client!.send(message);
  }

  private generateOperationId(): string {
    return String(++this.nextOperationId);
  }

  private async connect() {
    this.client = WS(this.url);
    try {
      this.sendMessage(
        undefined,
        MessageTypes.GQL_CONNECTION_INIT,
        await this.connectionParams()
      );
    } catch (err) {
      this.sendMessage(undefined, MessageTypes.GQL_CONNECTION_ERROR, {
        error: err
      });
    }

    this.client.on("disconnect", () => {
      this.unsubscribeAll();
      this.eventEmitter.emit("disconnected");
      this.client!.close();
      this.client = null;
    });

    this.client.on("connect_error", (err: Error) => {
      this.eventEmitter.emit("error", err);
    });

    this.client.on("error", (err: Error) => {
      this.eventEmitter.emit("error", err);
    });

    this.client.on("message", (data: IncomingMessage) => {
      this.processReceivedData(data);
    });
  }

  private processReceivedData(msg: IncomingMessage) {
    console.log("msg", msg);
    const opId = msg.id;

    if (
      [
        MessageTypes.GQL_DATA,
        MessageTypes.GQL_COMPLETE,
        MessageTypes.GQL_ERROR
      ].indexOf(msg.type) !== -1 &&
      !this.operations[opId]
    ) {
      this.unsubscribe(opId);

      return;
    }

    switch (msg.type) {
      case MessageTypes.GQL_CONNECTION_ERROR:
        if (this.connectionCallback) {
          this.connectionCallback(msg.payload as Error[], null);
        }
        break;

      case MessageTypes.GQL_CONNECTION_ACK:
        this.eventEmitter.emit("connected");

        if (this.connectionCallback) {
          this.connectionCallback(null, null);
        }
        break;

      case MessageTypes.GQL_COMPLETE:
        this.operations[opId]!.handler(null, null);
        delete this.operations[opId];
        break;

      case MessageTypes.GQL_ERROR:
        this.operations[opId]!.handler(
          this.formatErrors(msg.payload as OperationError),
          null
        );
        delete this.operations[opId];
        break;

      case MessageTypes.GQL_DATA:
        const parsedPayload: ExecutionResult = !msg.payload.errors
          ? msg.payload
          : {
              ...msg.payload,
              errors: this.formatErrors(msg.payload.errors)
            };
        this.operations[opId]!.handler(null, parsedPayload);
        break;

      default:
        throw new Error("Invalid message type!");
    }
  }

  private unsubscribe(opId: string) {
    if (this.operations[opId]) {
      delete this.operations[opId];
      this.sendMessage(opId, MessageTypes.GQL_STOP, {});
    }
  }
}
