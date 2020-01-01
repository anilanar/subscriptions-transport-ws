import * as WS from "socket.io";
import MessageTypes from "./message-types";
import isObject from "./utils/is-object";
import {
  parse,
  ExecutionResult,
  GraphQLSchema,
  DocumentNode,
  validate,
  ValidationContext,
  specifiedRules,
  GraphQLFieldResolver,
  ValidationRule
} from "graphql";
import { createEmptyIterable } from "./utils/empty-iterable";
import { createAsyncIterator, forAwaitEach, isAsyncIterable } from "iterall";
import { isASubscriptionOperation } from "./utils/is-subscriptions";
import { parseLegacyProtocolMessage } from "./legacy/parse-legacy-protocol";
import { IncomingMessage } from "http";
import { ExecutionResultDataDefault } from "graphql/execution/execute";

export interface TransportError {
  errors?: unknown;
  message?: string;
}

export type ExecutionIterator = AsyncIterableIterator<ExecutionResult>;

export interface OnOperation {
  (msg: OperationMessage, params: ExecutionParams<unknown>, socket: WS.Socket):
    | ExecutionParams<unknown>
    | Promise<ExecutionParams<unknown>>;
}

export interface ExecutionParams<TContext = unknown> {
  query: string | DocumentNode;
  variables: { [key: string]: unknown };
  operationName: string;
  context: TContext;
  formatResponse?: Function;
  formatError?: Function;
  callback?: Function;
  schema?: GraphQLSchema;
}

export type ConnectionContext = {
  initPromise?: Promise<unknown>;
  isLegacy: boolean;
  socket: WS.Socket;
  request: IncomingMessage;
  operations: {
    [opId: string]: ExecutionIterator;
  };
};

export interface OperationMessagePayload {
  [key: string]: unknown; // this will support for example unknown options sent in init like the auth token
  query?: string;
  variables?: { [key: string]: unknown };
  operationName?: string;
}

export interface OperationMessage {
  payload?: OperationMessagePayload;
  id?: string;
  type: string;
}

export type ExecuteFunction = (
  schema: GraphQLSchema,
  document: DocumentNode,
  rootValue?: unknown,
  contextValue?: unknown,
  variableValues?: { [key: string]: unknown },
  operationName?: string,
  fieldResolver?: GraphQLFieldResolver<unknown, unknown>
) =>
  | ExecutionResult
  | Promise<ExecutionResult>
  | AsyncIterator<ExecutionResult>;

export type SubscribeFunction = (
  schema: GraphQLSchema,
  document: DocumentNode,
  rootValue?: unknown,
  contextValue?: unknown,
  variableValues?: { [key: string]: unknown },
  operationName?: string,
  fieldResolver?: GraphQLFieldResolver<unknown, unknown>,
  subscribeFieldResolver?: GraphQLFieldResolver<unknown, unknown>
) =>
  | AsyncIterator<ExecutionResult>
  | Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;

export interface ServerOptions {
  rootValue?: unknown;
  schema?: GraphQLSchema;
  execute?: ExecuteFunction;
  subscribe?: SubscribeFunction;
  validationRules?: ReadonlyArray<ValidationRule>;
  onOperation?: OnOperation;
  onOperationComplete?: Function;
  onConnect?: Function;
  onDisconnect?: Function;
  keepAlive?: number;
}

const isSocketIOServer = (
  server: WS.Server | WS.ServerOptions
): server is WS.Server => "on" in server;

export class SubscriptionServer {
  private onOperation?: OnOperation;
  private onOperationComplete?: Function;
  private onConnect?: Function;
  private onDisconnect?: Function;

  private wsServer: WS.Server;
  private execute?: ExecuteFunction;
  private subscribe?: SubscribeFunction;
  private schema?: GraphQLSchema;
  private rootValue: unknown;
  private closeHandler: () => void;
  private specifiedRules: ReadonlyArray<ValidationRule>;

  public static create(
    options: ServerOptions,
    socketOptionsOrServer: WS.ServerOptions | WS.Server
  ) {
    return new SubscriptionServer(options, socketOptionsOrServer);
  }

  constructor(
    options: ServerOptions,
    socketOptionsOrServer: WS.ServerOptions | WS.Server
  ) {
    const {
      onOperation,
      onOperationComplete,
      onConnect,
      onDisconnect,
      keepAlive
    } = options;

    this.specifiedRules = options.validationRules || specifiedRules;
    this.loadExecutor(options);

    this.onOperation = onOperation;
    this.onOperationComplete = onOperationComplete;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;

    if (isSocketIOServer(socketOptionsOrServer)) {
      this.wsServer = socketOptionsOrServer;
    } else {
      // Init and connect WebSocket server to http
      this.wsServer = WS(socketOptionsOrServer || {});
    }

    const connectionHandler = (socket: WS.Socket, request: IncomingMessage) => {
      const connectionContext: ConnectionContext = Object.create(null);
      connectionContext.initPromise = Promise.resolve(true);
      connectionContext.isLegacy = false;
      connectionContext.socket = socket;
      connectionContext.request = request;
      connectionContext.operations = {};

      const connectionClosedHandler = (error: TransportError) => {
        if (error) {
          this.sendError(
            connectionContext,
            "",
            { message: error.message ? error.message : error },
            MessageTypes.GQL_CONNECTION_ERROR
          );

          setTimeout(() => {
            // 1011 is an unexpected condition prevented the request from being fulfilled
            connectionContext.socket.disconnect(true);
          }, 10);
        }
        this.onClose(connectionContext);

        if (this.onDisconnect) {
          this.onDisconnect(socket, connectionContext);
        }
      };

      socket.on("error", connectionClosedHandler);
      socket.on("close", connectionClosedHandler);
      socket.on("message", this.onMessage(connectionContext));
    };

    this.wsServer.on("connection", connectionHandler);
    this.closeHandler = () => {
      this.wsServer.of("/").removeListener("connection", connectionHandler);
      this.wsServer.close();
    };
  }

  public get server(): WS.Server {
    return this.wsServer;
  }

  public close(): void {
    this.closeHandler();
  }

  private loadExecutor(options: ServerOptions) {
    const { execute, subscribe, schema, rootValue } = options;

    if (!execute) {
      throw new Error(
        "Must provide `execute` for websocket server constructor."
      );
    }

    this.schema = schema;
    this.rootValue = rootValue;
    this.execute = execute;
    this.subscribe = subscribe;
  }

  private unsubscribe(connectionContext: ConnectionContext, opId: string) {
    if (connectionContext.operations && connectionContext.operations[opId]) {
      const { return: return_ } = connectionContext.operations[opId];
      if (return_) {
        return_();
      }

      delete connectionContext.operations[opId];

      if (this.onOperationComplete) {
        this.onOperationComplete(connectionContext.socket, opId);
      }
    }
  }

  private onClose(connectionContext: ConnectionContext) {
    Object.keys(connectionContext.operations).forEach(opId => {
      this.unsubscribe(connectionContext, opId);
    });
  }

  private onMessage(connectionContext: ConnectionContext) {
    return (message: unknown) => {
      let parsedMessage: OperationMessage;
      try {
        parsedMessage = parseLegacyProtocolMessage(connectionContext, message);
      } catch (e) {
        this.sendError(
          connectionContext,
          null,
          { message: e.message },
          MessageTypes.GQL_CONNECTION_ERROR
        );
        return;
      }

      const opId = parsedMessage.id!;
      switch (parsedMessage.type) {
        case MessageTypes.GQL_CONNECTION_INIT:
          if (this.onConnect) {
            connectionContext.initPromise = new Promise((resolve, reject) => {
              try {
                // TODO - this should become a function call with just 2 arguments in the future
                // when we release the breaking change api: parsedMessage.payload and connectionContext
                resolve(
                  this.onConnect!(
                    parsedMessage.payload,
                    connectionContext.socket,
                    connectionContext
                  )
                );
              } catch (e) {
                reject(e);
              }
            });
          }

          connectionContext
            .initPromise!.then(result => {
              if (result === false) {
                throw new Error("Prohibited connection!");
              }

              this.sendMessage(
                connectionContext,
                undefined,
                MessageTypes.GQL_CONNECTION_ACK,
                undefined
              );
            })
            .catch((error: Error) => {
              this.sendError(
                connectionContext,
                opId,
                { message: error.message },
                MessageTypes.GQL_CONNECTION_ERROR
              );

              // Close the connection with an error code, ws v2 ensures that the
              // connection is cleaned up even when the closing handshake fails.
              // 1011: an unexpected condition prevented the operation from being fulfilled
              // We are using setTimeout because we want the message to be flushed before
              // disconnecting the client
              setTimeout(() => {
                connectionContext.socket.disconnect(true);
              }, 10);
            });
          break;

        case MessageTypes.GQL_CONNECTION_TERMINATE:
          connectionContext.socket.disconnect(true);
          break;

        case MessageTypes.GQL_START:
          connectionContext
            .initPromise!.then(initResult => {
              // if we already have a subscription with this id, unsubscribe from it first
              if (
                connectionContext.operations &&
                connectionContext.operations[opId]
              ) {
                this.unsubscribe(connectionContext, opId);
              }

              const baseParams: ExecutionParams = {
                query: parsedMessage.payload!.query!,
                variables: parsedMessage.payload!.variables!,
                operationName: parsedMessage.payload!.operationName!,
                context: isObject(initResult)
                  ? Object.assign(
                      Object.create(Object.getPrototypeOf(initResult)),
                      initResult
                    )
                  : {},
                formatResponse: undefined,
                formatError: undefined,
                callback: undefined,
                schema: this.schema
              };
              let promisedParams = Promise.resolve(baseParams);

              // set an initial mock subscription to only registering opId
              connectionContext.operations[opId] = createEmptyIterable();

              if (this.onOperation) {
                let messageForCallback: OperationMessage = parsedMessage;
                promisedParams = Promise.resolve(
                  this.onOperation(
                    messageForCallback,
                    baseParams,
                    connectionContext.socket
                  )
                );
              }

              promisedParams
                .then(params => {
                  if (typeof params !== "object") {
                    const error = `Invalid params returned from onOperation! return values must be an object!`;
                    this.sendError(connectionContext, opId, { message: error });

                    throw new Error(error);
                  }

                  if (!params.schema) {
                    const error =
                      "Missing schema information. The GraphQL schema should be provided either statically in" +
                      " the `SubscriptionServer` constructor or as a property on the object returned from onOperation!";
                    this.sendError(connectionContext, opId, { message: error });

                    throw new Error(error);
                  }

                  const document =
                    typeof baseParams.query !== "string"
                      ? baseParams.query
                      : parse(baseParams.query);
                  let executionPromise: Promise<
                    AsyncIterator<ExecutionResult> | ExecutionResult
                  >;
                  const validationErrors = validate(
                    params.schema,
                    document,
                    this.specifiedRules
                  );

                  if (validationErrors.length > 0) {
                    executionPromise = Promise.resolve({
                      errors: validationErrors
                    });
                  } else {
                    let executor:
                      | SubscribeFunction
                      | ExecuteFunction
                      | undefined = this.execute;
                    if (
                      this.subscribe &&
                      isASubscriptionOperation(document, params.operationName)
                    ) {
                      executor = this.subscribe;
                    }
                    executionPromise = Promise.resolve(
                      executor!(
                        params.schema,
                        document,
                        this.rootValue,
                        params.context,
                        params.variables,
                        params.operationName
                      )
                    );
                  }

                  return executionPromise.then(executionResult => ({
                    executionIterable: isAsyncIterable(executionResult)
                      ? (executionResult as ExecutionIterator)
                      : ((createAsyncIterator([
                          executionResult as ExecutionResult
                        ]) as unknown) as ExecutionIterator),
                    params
                  }));
                })
                .then(({ executionIterable, params }) => {
                  forAwaitEach(executionIterable, (value: ExecutionResult) => {
                    let result = value;

                    if (params.formatResponse) {
                      try {
                        result = params.formatResponse(value, params);
                      } catch (err) {
                        console.error("Error in formatError function:", err);
                      }
                    }

                    this.sendMessage(
                      connectionContext,
                      opId,
                      MessageTypes.GQL_DATA,
                      result
                    );
                  })
                    .then(() => {
                      this.sendMessage(
                        connectionContext,
                        opId,
                        MessageTypes.GQL_COMPLETE,
                        null
                      );
                    })
                    .catch((e: Error) => {
                      let error = e;

                      if (params.formatError) {
                        try {
                          error = params.formatError(e, params);
                        } catch (err) {
                          console.error("Error in formatError function: ", err);
                        }
                      }

                      // plain Error object cannot be JSON stringified.
                      if (Object.keys(e).length === 0) {
                        error = { name: e.name, message: e.message };
                      }

                      this.sendError(connectionContext, opId, error);
                    });

                  return executionIterable;
                })
                .then((subscription: ExecutionIterator) => {
                  connectionContext.operations[opId] = subscription;
                })
                .then(() => {
                  // NOTE: This is a temporary code to support the legacy protocol.
                  // As soon as the old protocol has been removed, this coode should also be removed.
                  this.sendMessage(
                    connectionContext,
                    opId,
                    MessageTypes.SUBSCRIPTION_SUCCESS,
                    undefined
                  );
                })
                .catch((e: TransportError) => {
                  if (e.errors) {
                    this.sendMessage(
                      connectionContext,
                      opId,
                      MessageTypes.GQL_DATA,
                      { errors: e.errors }
                    );
                  } else {
                    this.sendError(connectionContext, opId, {
                      message: e.message
                    });
                  }

                  // Remove the operation on the server side as it will be removed also in the client
                  this.unsubscribe(connectionContext, opId);
                  return;
                });
              return promisedParams;
            })
            .catch(error => {
              // Handle initPromise rejected
              this.sendError(connectionContext, opId, {
                message: error.message
              });
              this.unsubscribe(connectionContext, opId);
            });
          break;

        case MessageTypes.GQL_STOP:
          // Find subscription id. Call unsubscribe.
          this.unsubscribe(connectionContext, opId);
          break;

        default:
          this.sendError(connectionContext, opId, {
            message: "Invalid message type!"
          });
      }
    };
  }

  private sendKeepAlive(connectionContext: ConnectionContext): void {
    if (connectionContext.isLegacy) {
      this.sendMessage(
        connectionContext,
        undefined,
        MessageTypes.KEEP_ALIVE,
        undefined
      );
    } else {
      this.sendMessage(
        connectionContext,
        undefined,
        MessageTypes.GQL_CONNECTION_KEEP_ALIVE,
        undefined
      );
    }
  }

  private sendMessage(
    connectionContext: ConnectionContext,
    opId: string | null | undefined,
    type: string,
    payload: unknown
  ): void {
    const parsedMessage = parseLegacyProtocolMessage(connectionContext, {
      type,
      id: opId,
      payload
    });

    connectionContext.socket.send(parsedMessage);
  }

  private sendError(
    connectionContext: ConnectionContext,
    opId: string | null | undefined,
    errorPayload: unknown,
    overrideDefaultErrorType?: string
  ): void {
    const sanitizedOverrideDefaultErrorType =
      overrideDefaultErrorType || MessageTypes.GQL_ERROR;
    if (
      [MessageTypes.GQL_CONNECTION_ERROR, MessageTypes.GQL_ERROR].indexOf(
        sanitizedOverrideDefaultErrorType
      ) === -1
    ) {
      throw new Error(
        "overrideDefaultErrorType should be one of the allowed error messages" +
          " GQL_CONNECTION_ERROR or GQL_ERROR"
      );
    }

    this.sendMessage(
      connectionContext,
      opId,
      sanitizedOverrideDefaultErrorType,
      errorPayload
    );
  }
}
