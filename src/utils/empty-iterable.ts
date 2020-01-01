import { $$asyncIterator } from "iterall";

export const createEmptyIterable = (): AsyncIterableIterator<never> => {
  return ({
    next() {
      return Promise.resolve({ value: undefined, done: true });
    },
    return() {
      return Promise.resolve({ value: undefined, done: true });
    },
    throw(e: Error) {
      return Promise.reject(e);
    },
    [$$asyncIterator]() {
      return this;
    }
  } as unknown) as AsyncIterableIterator<never>;
};
