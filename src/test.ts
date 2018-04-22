/* tslint:disable:no-conditional-assignment arrow-parens no-console */

import { QueryBuilder } from "squel";
import EBase, { IKVObject } from "./index";

function getAllMethodNames(obj: any) {
  const methods: Set<string> = new Set();
  while ((obj = Reflect.getPrototypeOf(obj))) {
    const keys = Reflect.ownKeys(obj);
    keys.forEach(k => methods.add(k as string));
  }
  return Array.from(methods);
}

const PARAMS_1: IKVObject<any> = {
  _count: [],
  _getByPrimary: ["1", ["a", "c"]],
  _getOneByField: [],
  _deleteByPrimary: ["2"],
  _deleteByField: [{ a: "b" }],
  _insert: [{ a: "b" }],
  _batchInsert: [[{ a: "b", a2: "b2" }]],
  _updateByPrimary: [1, { a: "b2" }],
  _createOrUpdate: [{ c: "1" }],
  _updateByField: [{ a: "b" }, { a: "c" }],
  _incrFields: [1, ["a"], 2],
  _list: [],
  _search: ["Yourtion", ["name"]],
};

class Base<T> extends EBase<T> {
  errorHandler(err: any) { throw err; }
  debugSQL(name: string)  { return (sql: any) => sql; }
  query(sql: string | QueryBuilder, connection: any) { return connection.queryAsync(sql); }
}

describe("Snapshot", () => {
  const base = new Base<string>("test", {}) as IKVObject;
  const methods = getAllMethodNames(base).filter(k => {
    return k.indexOf("_") === 0 && k.indexOf("__") === -1;
  });

  for (const method of methods) {
    test(method, () => {
      const param = PARAMS_1[method];
      const res = base[method](...param);
      expect(res.toString()).toMatchSnapshot();
    });
  }
});
