"use strict";

/**
 * @file base model 基础模块
 * @author Yourtion Guo <yourtion@gmail.com>
 */

import { Delete, Insert, MysqlInsert, QueryBuilder, Select, Update } from "squel";
import * as Squel from "squel";

export { Delete, Insert, MysqlInsert, Select, Update };

const SELETE_OPT = { autoQuoteTableNames: true, autoQuoteFieldNames: true };

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

export interface IKVObject {
  [key: string]: any;
}

export const squel = Squel.useFlavour("mysql");

/**
 * 删除对象中的 undefined
 *
 * @param {Object} object
 * @returns {Object}
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
 *
 * @param {Object} sql Squel 对象
 * @param {Object} conditions 查询条件
 */
function _parseWhere(sql: Select, conditions: IKVObject) {
  Object.keys(conditions).forEach((k) => {
    if (k.indexOf("$") === 0) {
      // 以 $ 开头直接解析
      if (Array.isArray(conditions[k])) {
        sql.where(conditions[k][0], ...conditions[k].slice(1));
      } else {
        sql.where(conditions[k]);
      }
    } else if (k.indexOf("#") !== -1) {
      sql.where(`${k.replace("#", "")} like ?`, "%" + conditions[k] + "%");
    } else if (k.indexOf("$") !== -1) {
      sql.where(k.replace("$", ""), conditions[k]);
    } else if (Array.isArray(conditions[k])) {
      // 数组类型使用 in 方式
      sql.where(`${k} in ?`, conditions[k]);
    } else {
      // 使用查询条件解析
      sql.where(`${k} = ?`, conditions[k]);
    }
  });
}

/**
 * 数据库错误处理
 *
 * @param {Error} err 错误
 */
function errorHandler(err: any) {
  // 如果是自定义错误直接抛出
  if (err.code && !isNaN(err.code - 0)) {
    throw err;
  }
  // 判断条件
  switch (err.code) {
    case "ER_DUP_ENTRY":
      throw Error("ER_DUP_ENTRY");
    default:
      throw err;
  }
}

export interface IBaseOptions {
  prefix?: string;
  primaryKey?: string;
  fields?: string[];
  order?: string;
}

export default class Base<T> {
  public table: string;
  public primaryKey: string;
  public fields: string[];
  public order?: string;
  public _parseWhere = _parseWhere;
  public connect: any;

  /**
   * Creates an instance of Base.
   * @param {String} table 表名
   * @param {Object} [options={}]
   *   - {Object} fields 默认列
   *   - {Object} order 默认排序字段
   * @memberof Base
   */
  constructor(table: string, connect, options: IBaseOptions = {}) {
    const tablePrefix = options.prefix !== undefined ? options.prefix : "";
    this.table = tablePrefix ? tablePrefix + table : table;
    this.primaryKey = options.primaryKey || "id";
    this.fields = options.fields || [];
    this.order = options.order;
    this.connect = connect;
  }

  /**
   * 输出 SQL Debug
   *
   * @param {String} name Debug 前缀
   * @returns {String} SQL
   * @memberof Base
   */
  public debugSQL(name: string) {
    return (sql: any) => {
      // mysqlLogger.debug(` ${name} : ${sql}`);
      return sql;
    };
  }

  /**
   * 查询方法（内部查询尽可能调用这个，会打印Log）
   *
   * @param {String} sql SQL字符串
   * @param {Object} [connection=mysql] Mysql连接，默认为pool
   * @returns {Promise}
   * @memberof Base
   */
  public query(sql: QueryBuilder | string, connection = this.connect) {
    if (typeof sql === "string") {
      // mysqlLogger.debug(sql);
      return connection.queryAsync(sql).catch((err) => errorHandler(err));
    }
    const { text, values } = sql.toParam();
    // mysqlLogger.debug(text, values);
    // mysqlLogger.trace(sql.toString());
    return connection.queryAsync(text, values).catch((err) => errorHandler(err));
  }

  public _count(conditions: IKVObject = {}) {
    const sql = squel
      .select()
      .from(this.table)
      .field("COUNT(*)", "c");
    _parseWhere(sql, conditions);
    return sql;
  }

  /**
   * 计算数据表 count
   *
   * @param {Object} [conditions={}] 条件
   * @returns {Promise}
   * @memberof Base
   */
  public count(conditions: IKVObject = {}): Promise<number> {
    return this.query(this._count(conditions)).then((res) => res && res[0] && res[0].c);
  }

  public _getByPrimary(primary: string | number, fields: string[]) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    const sql = squel
      .select(SELETE_OPT)
      .from(this.table)
      .where(this.primaryKey + " = ?", primary)
      .limit(1);
    fields.forEach((f) => sql.field(f));
    return sql;
  }

  /**
   * 根据 ID 获取数据
   *
   * @param {Number} primary 主键
   * @param {Array} [fields=this.fields] 所需要的列数组
   * @returns {Promise}
   * @memberof Base
   */
  public getByPrimary(primary: string, fields = this.fields): Promise<T> {
    return this.query(this._getByPrimary(primary, fields)).then((res) => res && res[0]);
  }

  public _getOneByField(object: IKVObject = {}, fields = this.fields) {
    const sql = squel
      .select(SELETE_OPT)
      .from(this.table)
      .limit(1);
    fields.forEach((f) => sql.field(f));
    _parseWhere(sql, object);
    return sql;
  }

  /**
   * 根据查询条件获取一条记录
   *
   * @param {Object} [object={}] 字段、值对象
   * @param {Array} [fields=this.fields] 所需要的列数组
   * @returns {Promise}
   * @memberof Base
   */
  public getOneByField(object: IKVObject = {}, fields = this.fields): Promise<T> {
    return this.query(this._getOneByField(object, fields)).then((res) => res && res[0]);
  }

  public _deleteByPrimary(primary: string | number, limit = 1) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    return squel
      .delete()
      .from(this.table)
      .where(this.primaryKey + " = ?", primary)
      .limit(limit);
  }

  /**
   * 根据主键删除数据
   *
   * @param {Number} primary 主键
   * @param {Number} [limit=1] 删除条数
   * @returns {Promise}
   * @memberof Base
   */
  public deleteByPrimary(primary: string | number, limit = 1): Promise<number> {
    return this.query(this._deleteByPrimary(primary, limit)).then((res) => res && res.affectedRows);
  }

  public _deleteByField(conditions: IKVObject, limit = 1) {
    const sql = squel
      .delete()
      .from(this.table)
      .limit(limit);
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
  public deleteByField(conditions: IKVObject, limit = 1): Promise<number> {
    return this.query(this._deleteByField(conditions, limit)).then((res) => res && res.affectedRows);
  }

  /**
   * 根据查询条件获取记录
   *
   * @param {Object} [object={}] 字段、值对象
   * @param {Array} [fields=this.fields] 所需要的列数组
   * @returns {Promise}
   * @memberof Base
   */
  public getByField(conditions: IKVObject = {}, fields = this.fields): Promise<T[]> {
    return this.list(conditions, fields, 999);
  }

  public _insert(object: IKVObject = {}) {
    removeUndefined(object);
    return squel
      .insert()
      .into(this.table)
      .setFields(object);
  }

  /**
   * 插入一条数据
   *
   * @param {Object} [object={}] 插入的数据对象
   * @returns {Promise}
   * @memberof Base
   */
  public insert(object: IKVObject = {}) {
    return this.query(this._insert(object));
  }

  public _batchInsert(array: IKVObject[]) {
    array.forEach((o) => removeUndefined(o));
    return squel
      .insert()
      .into(this.table)
      .setFieldsRows(array);
  }

  /**
   * 批量插入数据
   *
   * @param {Array<Object>} array 插入的数据对象数组
   * @returns {Promise}
   * @memberof Base
   */
  public batchInsert(array: IKVObject[]) {
    return this.query(this._batchInsert(array));
  }

  public _updateByPrimary(primary: string | number, objects: IKVObject, raw = false) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    removeUndefined(objects);
    const sql = squel
      .update()
      .table(this.table)
      .where(this.primaryKey + " = ?", primary);
    if (raw) {
      Object.keys(objects).forEach((k) => {
        if (k.indexOf("$") === 0) {
          sql.set(objects[k]);
        } else {
          sql.set(`${k} = ?`, objects[k]);
        }
      });
    } else {
      sql.setFields(objects);
    }
    return sql;
  }

  /**
   * 根据主键更新记录
   *
   * @param {Number} primary 主键
   * @param {Object} objects 更新的内容对象
   * @param {Boolean} raw 是否解析 field 对象
   * @returns {Promise}
   * @memberof Base
   */
  public updateByPrimary(primary: string | number, objects: IKVObject, raw = false): Promise<number> {
    return this.query(this._updateByPrimary(primary, objects, raw)).then((res) => res && res.affectedRows);
  }

  public _createOrUpdate(objects: IKVObject, update: string[]) {
    removeUndefined(objects);
    const sql = squel.insert().into(this.table);
    sql.setFields(objects);
    update.forEach((k) => {
      if (objects[k] !== undefined) {
        sql.onDupUpdate(k, objects[k]);
      } else if (Array.isArray(objects[k])) {
        sql.onDupUpdate(objects[k][0], objects[k][1]);
      }
    });
    return sql;
  }

  /**
   * 创建一条记录，如果存在就更新
   *
   * @param {Object} objects 创建记录对象
   * @param {Array} update 更新字段
   * @returns {Promise}
   * @memberof Base
   */
  public createOrUpdate(objects: IKVObject, update: string[]) {
    return this.query(this._createOrUpdate(objects, update));
  }

  public _updateByField(conditions: IKVObject, objects: IKVObject, raw = false) {
    if (!conditions || Object.keys(conditions).length < 1) {
      throw new Error("`key` 不能为空");
      // throw errors.dataBaseError("`key` 不能为空");
    }
    removeUndefined(objects);
    const sql = squel.update().table(this.table);
    if (raw) {
      Object.keys(objects).forEach((k) => {
        if (k.indexOf("$") === 0) {
          sql.set(objects[k]);
        } else {
          sql.set(`${k} = ?`, objects[k]);
        }
      });
    } else {
      sql.setFields(objects);
    }
    Object.keys(conditions).forEach((k) => sql.where(`${k} = ?`, conditions[k]));
    return sql;
  }

  /**
   * 根据查询条件更新记录
   *
   * @param {Object} key 查询条件对象
   * @param {Object} fields 更新的内容对象
   * @returns {Promise}
   * @memberof Base
   */
  public updateByField(conditions: IKVObject, objects: IKVObject, raw = false): Promise<number> {
    return this.query(this._updateByField(conditions, objects)).then((res) => res && res.affectedRows);
  }

  public _incrFields(primary: string | number, fields: string[], num = 1) {
    if (primary === undefined) {
      throw new Error("`primary` 不能为空");
      // throw errors.dataBaseError("`primary` 不能为空");
    }
    const sql = squel
      .update()
      .table(this.table)
      .where(this.primaryKey + " = ?", primary);
    fields.forEach((f) => sql.set(`${f} = ${f} + ${num}`));
    return sql;
  }

  /**
   * 根据主键对数据列执行加一操作
   *
   * @param {Number} primary 主键
   * @param {Array} fields 需要更新的列数组
   * @returns {Promise}
   * @memberof Base
   */
  public incrFields(primary: string | number, fields: string[], num = 1): Promise<number> {
    return this.query(this._incrFields(primary, fields, num)).then((res) => res && res.affectedRows);
  }

  public _list(
    conditions: IKVObject = {},
    fields = this.fields,
    limit = 999,
    offset = 0,
    order = this.order,
    asc = true,
  ) {
    removeUndefined(conditions);
    const sql = squel
      .select(SELETE_OPT)
      .from(this.table)
      .offset(offset)
      .limit(limit);
    fields.forEach((f) => sql.field(f));
    _parseWhere(sql, conditions);
    if (order) {
      sql.order(order, asc);
    }
    return sql;
  }

  /**
   * 根据条件获取列表
   *
   * @param {Object} [conditions={}] 查询条件对象
   * @param {Array} [fields=this.fields] 需要查询的字段
   * @param {IPageParams} pages 分页对象
   * @returns {Promise}
   * @memberof Base
   */
  public list(conditions: IKVObject, fields?: string[], pages?: IPageParams): Promise<T[]>;
  /**
   * 根据条件获取列表
   *
   * @param {Object} [conditions={}] 查询条件对象
   * @param {Array} [fields=this.fields] 需要查询的字段
   * @param {Number} [limit=999] 限制条数(999)
   * @param {Number} [offset=0] 跳过数量(0)
   * @param {String} [order=this.order] 排序字段(this.order
   * @param {Boolean} [asc=true] 是否正向排序(true)
   * @returns {Promise}
   * @memberof Base
   */
  public list(
    conditions: IKVObject,
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
    const sql = squel
      .select(SELETE_OPT)
      .from(this.table)
      .offset(offset)
      .limit(limit);
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
   *
   * @param {String} keyword 关键词
   * @param {Array} search 搜索字段
   * @param {Array} [fields=this.fields] 需要查询的字段
   * @param {IPageParams} pages 分页对象
   * @returns {Promise}
   * @memberof Base
   */
  public search(keyword: string, search: string[], fields?: string[], pages?: IPageParams): Promise<T[]>;
  /**
   * 根据关键词进行搜索
   *
   * @param {String} keyword 关键词
   * @param {Array} search 搜索字段
   * @param {Array} [fields=this.fields] 需要查询的字段
   * @param {Number} [limit=999] 限制条数(999)
   * @param {Number} [offset=0] 跳过数量(0)
   * @param {String} [order=this.order] 排序字段(this.order
   * @param {Boolean} [asc=true] 是否正向排序(true)
   * @returns {Promise}
   * @memberof Base
   */
  public search(
    keyword: string,
    search: string[],
    fields?: string[],
    limit?: number,
    offset?: number,
    order?: string,
    asc?: boolean,
  ): Promise<T[]>;
  public search(keyword: string, search: string[], fields = this.fields, ...args: any[]): Promise<T[]> {
    if (args.length === 1 && typeof args[0] === "object") {
      return this.query(
        this._search(keyword, search, fields, args[0].limit, args[0].offset, args[0].order, args[0].asc),
      );
    }
    return this.query(this._search(keyword, search, fields, ...args));
  }

  /**
   * 根据条件获取分页内容（比列表多处总数计算）
   *
   * @param {Object} [conditions={}] 查询条件对象
   * @param {Array} [fields=this.fields] 需要查询的字段
   * @param {Number} [limit=999] 限制条数(999)
   * @param {Number} [offset=0] 跳过数量(0)
   * @param {String} [order=this.order] 排序字段(this.order
   * @param {Boolean} [asc=true] 是否正向排序(true)
   * @returns {Promise}
   * @memberof Base
   */
  public page(
    conditions: IKVObject,
    fields?: string[],
    limit?: number,
    offset?: number,
    order?: string,
    asc?: boolean,
  ): Promise<IPageResult<T>>;
  /**
   * 根据条件获取分页内容（比列表多处总数计算）
   *
   * @param {Object} [conditions={}] 查询条件对象
   * @param {Array} [fields=this.fields] 需要查询的字段
   * @param {IPageParams} pages 分页对象
   * @returns {Promise}
   * @memberof Base
   */
  public page(conditions: IKVObject, fields?: string[], pages?: IPageParams): Promise<IPageResult<T>>;
  public page(conditions = {}, fields = this.fields, ...args: any[]): Promise<IPageResult<T>> {
    const listSql = this.list(conditions, fields, ...args);
    const countSql = this.count(conditions);
    return Promise.all([listSql, countSql]).then(([list, count = 0]) => list && { count, list });
  }

  /**
   * 执行事务（通过传人方法）
   *
   * @param {String} name
   * @param {Function} func
   * @memberof Base
   */
  public transactions(name, func) {
    return async () => {
      if (!name) {
        throw new Error("`name` 不能为空");
        // throw errors.dataBaseError('`name` 不能为空');
      }
      // utils.randomString(6);
      const tid = "";
      const debug = this.debugSQL(`Transactions[${tid}] - ${name}`);
      const connection = await this.connect.getConnectionAsync();
      connection.debugQuery = (sql) => {
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
        errorHandler(err);
      } finally {
        connection.release();
      }
    };
  }

  /**
   * 执行事务（通过传人SQL语句数组）
   *
   * @param {Array<String>} sqls SQL语言数组
   * @returns {Promise}
   * @memberof Base
   */
  public transactionSQLs(sqls) {
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
        errorHandler(err);
      } finally {
        await connection.release();
      }
    };
  }
}
