/**
 * @file base model 基础模块
 * @author Yourtion Guo <yourtion@gmail.com>
 */

import { Delete, Insert, MysqlInsert, QueryBuilder, Select, Update } from "squel";
import * as Squel from "squel";

export const squel = Squel.useFlavour("mysql");

export { Delete, Insert, MysqlInsert, Select, Update };

const SELETE_OPT = { autoQuoteTableNames: true, autoQuoteFieldNames: true };

export interface IKVObject<T = any> {
  [key: string]: T;
}
export type IConditions = IKVObject<string | string[]>;
export type IPrimary = string | number;

export interface IPageParams {
  limit?: number;
  offset?: number;
  order?: string;
  asc?: boolean;
}

export interface IPageResult<T> {
  count: number;
  list: T[];
}

/**
 * 删除对象中的 undefined
 */
function removeUndefined(object: IKVObject) {
  Object.keys(object).forEach((key) => object[key] === undefined && delete object[key]);
  if (Object.keys.length === 0) {
    throw Error("Object is empty");
  }
  return object;
}

/**
 * 解析 Where
 * - key-value 直接使用 =
 * - 以 $ 开头直接解析（数组直接析构）
 * - 以 # 开头解析为 like %*%
 * - 数组类型使用 in 方式
 */
function parseWhere(sql: Select, conditions: IConditions) {
  Object.keys(conditions).forEach((k) => {
    const condition = conditions[k];
    if (k.indexOf("$") === 0) {
      // 以 $ 开头直接解析
      if (Array.isArray(condition)) {
        sql.where(condition[0], ...condition.slice(1));
      } else {
        sql.where(condition);
      }
    } else if (k.indexOf("#") !== -1) {
      // 以 # 开头解析为 like
      sql.where(`${k.replace("#", "")} like ?`, "%" + condition + "%");
    } else if (k.indexOf("$") !== -1) {
      sql.where(k.replace("$", ""), condition);
    } else if (Array.isArray(condition)) {
      // 数组类型使用 in 方式
      sql.where(`${k} in ?`, condition);
    } else {
      // 使用查询条件解析
      sql.where(`${k} = ?`, condition);
    }
  });
}

export interface IBaseOptions {
  /** 表前缀 */
  prefix?: string;
  /** 主键名（默认为 id ） */
  primaryKey?: string;
  /** 默认 query 的列 */
  fields?: string[];
  /** 默认排序字段 */
  order?: string;
  /** 默认asc */
  asc?: boolean;
}

export default abstract class EBase<T> {

  public table: string;
  public primaryKey: string;
  public fields: string[];
  public parseWhere = parseWhere;
  public connect: any;
  private order?: string;
  private asc: boolean;

  constructor(table: string, connect: any, options: IBaseOptions = {}) {
    const tablePrefix = options.prefix !== undefined ? options.prefix : "";
    this.table = tablePrefix + table;
    this.primaryKey = options.primaryKey || "id";
    this.fields = options.fields || [];
    this.order = options.order;
    this.asc = options.asc || true;
    this.connect = connect;
  }

  /**
   * 输出 SQL Debug
   */
  abstract debugSQL<U = string>(name: string): (sql: U) => U;

  /**
   * 查询方法（内部查询尽可能调用这个，会打印Log）
   */
  abstract query(sql: QueryBuilder | string, connection?: any): any;

  abstract errorHandler(err: any): void;

  public _count(conditions: IConditions = {}) {
    const sql = squel.select().from(this.table).field("COUNT(*)", "c");
    parseWhere(sql, conditions);
    return sql;
  }

  /**
   * 计算数据表 count
   */
  public count(conditions: IConditions = {}): Promise<number> {
    return this.query(this._count(conditions)).then((res: any) => res && res[0] && res[0].c);
  }

  public _getByPrimary(primary: IPrimary, fields: string[]) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    const sql = squel.select(SELETE_OPT).from(this.table).where(this.primaryKey + " = ?", primary).limit(1);
    fields.forEach((f) => sql.field(f));
    return sql;
  }

  /**
   * 根据主键获取数据
   */
  public getByPrimary(primary: string, fields = this.fields): Promise<T> {
    return this.query(this._getByPrimary(primary, fields)).then((res: T[]) => res && res[0]);
  }

  public _getOneByField(object: IKVObject = {}, fields = this.fields) {
    const sql = squel.select(SELETE_OPT).from(this.table).limit(1);
    fields.forEach((f) => sql.field(f));
    parseWhere(sql, object);
    return sql;
  }

  /**
   * 根据查询条件获取一条记录
   */
  public getOneByField(object: IKVObject = {}, fields = this.fields): Promise<T> {
    return this.query(this._getOneByField(object, fields)).then((res: T[]) => res && res[0]);
  }

  public _deleteByPrimary(primary: IPrimary, limit = 1) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    return squel.delete().from(this.table).where(this.primaryKey + " = ?", primary).limit(limit);
  }

  /**
   * 根据主键删除数据
   */
  public deleteByPrimary(primary: IPrimary, limit = 1): Promise<number> {
    return this.query(this._deleteByPrimary(primary, limit)).then((res: any) => res && res.affectedRows);
  }

  public _deleteByField(conditions: IConditions, limit = 1) {
    const sql = squel.delete().from(this.table).limit(limit);
    Object.keys(conditions).forEach((k) =>
      sql.where(k + (Array.isArray(conditions[k]) ? " in" : " =") + " ? ", conditions[k]),
    );
    return sql;
  }

  /**
   * 根据查询条件删除数据
   *
   * @param {Object} [object={}] 字段、值对象
   * @param {Number} [limit=1] 删除条数
   * @returns {Promise}
   * @memberof Base
   */
  public deleteByField(conditions: IConditions, limit = 1): Promise<number> {
    return this.query(this._deleteByField(conditions, limit)).then((res: any) => res && res.affectedRows);
  }

  /**
   * 根据查询条件获取记录
   *
   * @param {Object} [object={}] 字段、值对象
   * @param {Array} [fields=this.fields] 所需要的列数组
   * @returns {Promise}
   * @memberof Base
   */
  public getByField(conditions: IConditions = {}, fields = this.fields): Promise<T[]> {
    return this.list(conditions, fields, 999);
  }

  public _insert(object: IKVObject = {}) {
    removeUndefined(object);
    return squel.insert().into(this.table).setFields(object);
  }

  /**
   * 插入一条数据
   */
  public insert(object: IKVObject = {}) {
    return this.query(this._insert(object));
  }

  public _batchInsert(array: IKVObject[]) {
    array.forEach((o) => removeUndefined(o));
    return squel.insert().into(this.table).setFieldsRows(array);
  }

  /**
   * 批量插入数据
   */
  public batchInsert(array: IKVObject[]) {
    return this.query(this._batchInsert(array));
  }

  public _updateByPrimary(primary: IPrimary, objects: IKVObject, raw = false) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    removeUndefined(objects);
    const sql = squel.update().table(this.table).where(this.primaryKey + " = ?", primary);
    if (!raw) {
      return sql.setFields(objects);
    }
    Object.keys(objects).forEach((k) => {
      if (k.indexOf("$") === 0) {
        sql.set(objects[k]);
      } else {
        sql.set(`${k} = ?`, objects[k]);
      }
    });
    return sql;
  }

  /**
   * 根据主键更新记录
   */
  public updateByPrimary(primary: IPrimary, objects: IKVObject, raw = false): Promise<number> {
    return this.query(this._updateByPrimary(primary, objects, raw)).then((res: any) => res && res.affectedRows);
  }

  public _createOrUpdate(objects: IKVObject, update = Object.keys(objects)) {
    removeUndefined(objects);
    const sql = squel.insert().into(this.table);
    sql.setFields(objects);
    update.forEach((k) => {
      if (Array.isArray(objects[k])) {
        sql.onDupUpdate(objects[k][0], objects[k][1]);
      } else if (objects[k] !== undefined) {
        sql.onDupUpdate(k, objects[k]);
      }
    });
    return sql;
  }

  /**
   * 创建一条记录，如果存在就更新
   */
  public createOrUpdate(objects: IKVObject, update = Object.keys(objects)) {
    return this.query(this._createOrUpdate(objects, update));
  }

  public _updateByField(conditions: IConditions, objects: IKVObject, raw = false) {
    if (!conditions || Object.keys(conditions).length < 1) {
      throw new Error("`key` 不能为空");
      // throw errors.dataBaseError("`key` 不能为空");
    }
    removeUndefined(objects);
    const sql = squel.update().table(this.table);
    Object.keys(conditions).forEach((k) => sql.where(`${k} = ?`, conditions[k]));
    if (!raw) {
      return sql.setFields(objects);
    }
    Object.keys(objects).forEach((k) => {
      if (k.indexOf("$") === 0) {
        sql.set(objects[k]);
      } else {
        sql.set(`${k} = ?`, objects[k]);
      }
    });
    return sql;
  }

  /**
   * 根据查询条件更新记录
   */
  public updateByField(conditions: IConditions, objects: IKVObject, raw = false): Promise<number> {
    return this.query(this._updateByField(conditions, objects)).then((res: any) => res && res.affectedRows);
  }

  public _incrFields(primary: IPrimary, fields: string[], num = 1) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    const sql = squel.update().table(this.table).where(this.primaryKey + " = ?", primary);
    fields.forEach((f) => sql.set(`${f} = ${f} + ${num}`));
    return sql;
  }

  /**
   * 根据主键对数据列执行加一操作
   */
  public incrFields(primary: IPrimary, fields: string[], num = 1): Promise<number> {
    return this.query(this._incrFields(primary, fields, num)).then((res: any) => res && res.affectedRows);
  }

  public _list(
    conditions: IConditions = {},
    fields = this.fields,
    limit = 999,
    offset = 0,
    order = this.order,
    asc = this.asc,
  ) {
    removeUndefined(conditions);
    const sql = squel.select(SELETE_OPT).from(this.table).offset(offset).limit(limit);
    fields.forEach((f) => sql.field(f));
    parseWhere(sql, conditions);
    if (order) {
      sql.order(order, asc);
    }
    return sql;
  }

  /**
   * 根据条件获取列表
   */
  public list(conditions: IConditions, fields?: string[], pages?: IPageParams): Promise<T[]>;
  /**
   * 根据条件获取列表
   */
  public list(
    conditions: IConditions,
    fields?: string[],
    limit?: number,
    offset?: number,
    order?: string,
    asc?: boolean,
  ): Promise<T[]>;
  public list(conditions = {}, fields = this.fields, ...args: any[]): Promise<T[]> {
    if (args.length === 1 && typeof args[0] === "object") {
      return this.query(this._list(conditions, fields, args[0].limit, args[0].offset, args[0].order, args[0].asc));
    }
    return this.query(this._list(conditions, fields, ...args));
  }

  public _search(
    keyword: string,
    search: string[],
    fields = this.fields,
    limit = 10,
    offset = 0,
    order = this.order,
    asc = true,
  ) {
    if (!keyword || search.length < 1) {
      throw new Error("`keyword` | `search` 不能为空");
      // throw errors.dataBaseError("`keyword` | `search` 不能为空");
    }
    const sql = squel.select(SELETE_OPT).from(this.table).offset(offset).limit(limit);
    fields.forEach((f) => sql.field(f));
    const exp = squel.expr();
    search.forEach((k) => {
      exp.or(`${k} like ?`, "%" + keyword + "%");
    });
    sql.where(exp);
    if (order) {
      sql.order(order, asc);
    }
    return sql;
  }

  /**
   * 根据关键词进行搜索
   */
  public search(keyword: string, search: string[], fields?: string[], pages?: IPageParams): Promise<T[]>;
  /**
   * 根据关键词进行搜索
   */
  public search(keyword: string, search: string[], fields?: string[], limit?: number, offset?: number, order?: string, asc?: boolean): Promise<T[]>;
  public search(keyword: string, search: string[], fields = this.fields, ...args: any[]): Promise<T[]> {
    if (args.length === 1 && typeof args[0] === "object") {
      return this.query(
        this._search(keyword, search, fields, args[0].limit, args[0].offset, args[0].order, args[0].asc),
      );
    }
    return this.query(this._search(keyword, search, fields, ...args));
  }

  /**
   * 根据条件获取分页内容（比列表多出总数计算）
   */
  public page(conditions: IConditions, fields?: string[], limit?: number, offset?: number, order?: string, asc?: boolean): Promise<IPageResult<T>>;
  /**
   * 根据条件获取分页内容（比列表多出总数计算）
   */
  public page(conditions: IConditions, fields?: string[], pages?: IPageParams): Promise<IPageResult<T>>;
  public page(conditions = {}, fields = this.fields, ...args: any[]): Promise<IPageResult<T>> {
    const listSql = this.list(conditions, fields, ...args);
    const countSql = this.count(conditions);
    return Promise.all([listSql, countSql]).then(([list, count = 0]) => list && { count, list });
  }

  /**
   * 执行事务（通过传人方法）
   */
  public transactions(name: string, func: (conn: any) => any) {
    return async () => {
      if (!name) {
        throw new Error("`name` 不能为空");
        // throw errors.dataBaseError('`name` 不能为空');
      }
      // utils.randomString(6);
      const tid = "";
      const debug = this.debugSQL(`Transactions[${tid}] - ${name}`);
      const connection = await this.connect.getConnectionAsync();
      connection.debugQuery = (sql: any) => {
        debug(sql);
        return connection.queryAsync(sql);
      };
      await connection.beginTransactionAsync(); // 开始事务
      debug("Transaction Begin");
      try {
        const result = await func(connection);
        await connection.commitAsync(); // 提交事务
        // debug('result: ', result);
        // debug('Transaction Done');
        return result;
      } catch (err) {
        // 回滚错误
        // console.log(err);
        await connection.rollbackAsync();
        // debug('Transaction Rollback', err.code < 0);
        this.errorHandler(err);
      } finally {
        connection.release();
      }
    };
  }

  /**
   * 执行事务（通过传人SQL语句数组）
   */
  public transactionSQLs(sqls: string[]) {
    return async () => {
      if (!sqls || sqls.length < 1) {
        throw new Error("`sqls` 不能为空");
        // throw errors.dataBaseError('`sqls` 不能为空');
      }
      // logger.debug('Begin Transaction');
      const connection = await this.connect.getConnectionAsync();
      await connection.beginTransactionAsync();
      try {
        for (const sql of sqls) {
          // logger.debug(`Transaction SQL: ${ sql }`);
          await connection.queryAsync(sql);
        }
        const res = await connection.commitAsync();
        // logger.debug('Done Transaction');
        return res;
      } catch (err) {
        await connection.rollbackAsync();
        // logger.debug('Rollback Transaction');
        this.errorHandler(err);
      } finally {
        await connection.release();
      }
    };
  }
}
