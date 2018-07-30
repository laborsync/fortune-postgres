'use strict'

const pgPool = require('pg-pool');
const url = require('url');
const querystring = require('querystring');
const crypto = require('crypto');

const randomBytes = crypto.randomBytes;

const idSymbols = '-_';
const idDigits = '0123456789';
const idLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
const idCharset = [idSymbols, idDigits, idLetters, idLetters.toLowerCase()].join('');

const logicalOperators = ['and', 'or'];

module.exports = Adapter => class PostgresAdapter extends Adapter {

    pool() {

        const options = this.options;

        if (!this._pool) {

            if (options.url === undefined) {
                throw new Error('A url is required to connect');
            }

            const params = url.parse(options.url);
            const auth = params.auth.split(':');

            const query = querystring.parse(params.query);

            const ssl = query.ssl !== 'false';

            const config = {
                user: auth[0],
                password: auth[1],
                host: params.hostname,
                port: params.port,
                database: params.pathname.split('/')[1],
                ssl,
            };

            this._pool = new pgPool(config);
        }

        return this._pool;

    }

    _computeId(type) {
        return randomBytes(20).map(
            value => idCharset.charCodeAt(
                Math.floor(value * idCharset.length / 256)
            )
        ).toString();
    }

    _computeSelect(type, options) {

        options = options || {};
        options.fields = options.fields || {}

        const recordTypes = this.recordTypes;
        const denormalizedInverseKey = this.keys.denormalizedInverse

        let fields = Object.keys(options.fields).filter((field) => {
            return !(denormalizedInverseKey in recordTypes[type][field]) && recordTypes[type][field];
        });

        if (!fields.length) {
            fields = Object.keys(recordTypes[type]);
        }

        const typeId = this._computeTypeId(type);
        fields.unshift(typeId);
        
        const selects = fields.map((field, index) => {
            return '"' + this._computeTypeColumn(type, field) + (index && recordTypes[type][field].type === String ? '"::text' : '"') + ' AS "' + (!index ? 'id' : field) + '"';
        });

        return 'SELECT ' + selects.join(', ');

    }

    _computeTypeId(type) {

        const typeMap = this.options.typeMap || {};

        if (!typeMap[type].id === undefined) {
            throw new Error('A typeMap id is required for type "' + type + '"');
        }

        return typeMap[type].id;

    }

    _computeTypeColumn(type, field) {
        const typeMap = this.options.typeMap || {};
        return typeMap[type].column === undefined ? field : typeMap[type].column(field);
    }

    _computeTable(type) {

        const typeMap = this.options.typeMap || {};
        if (typeof typeMap[type].table !== 'string') {
            throw new Error('A typeMap table is required for type "' + type + '"');
        }

        return '"' + typeMap[type].table + '"';

    }

    _computeTypeNumberKeys(type) {

        const recordTypes = this.recordTypes;

        return Object.keys(recordTypes[type]).filter((key) => {
            return recordTypes[type][key].type === Number;
        });

    }

    _computeFrom(type, ids, options) {
        return 'FROM ' + this._computeTable(type, options);
    }

    _computeWhere(type, ids, options, meta, params, root = true) {

        const where = this._computeWhereLogicalOperators(type, options, meta, params, [
            this._computeWhereIds(type, ids, params),
            this._computeWhereExists(type, options, meta, params),
            this._computeWhereMatch(type, options, meta, params),
            this._computeWhereRange(type, options, meta, params),
        ].filter(Boolean).join(' AND '));

        return where ? (root ? 'WHERE ' + where : '(' + where + ')') : null;

    }

    _computeSort(type, options) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const sort = [];
        for (const field in options.sort) {

            const isArray = recordTypes[type][field] && recordTypes[type][field][isArrayKey];
            const order = options.sort[field] ? 'ASC' : 'DESC';

            if (!isArray) {
                sort.push('"' + this._computeTypeColumn(type, field) + '" ' + order);
            } else {
                sort.push('COALESCE(ARRAY_LENGTH("' + this._computeTypeColumn(type, field) + '", 1), 0) ' + order);
            }

        }

        return sort.length ? ('ORDER BY ' + sort.join(', ')) : null;

    }

    _computeLimit(options) {
        return options.limit ? 'LIMIT ' + options.limit : null;
    }

    _computeOffset(options) {
        return options.offset ? 'OFFSET ' + options.offset : null;
    }

    _computeWhereIds(type, ids, params) {

        const whereIds = [];

        if (ids) {

            if (!Array.isArray(ids)) {
                ids = [ids];
            }

            ids.map((id) => {
                params.push(id);
                whereIds.push('$' + params.length);
            });

        }

        return whereIds.length ? '"' + this._computeTypeId(type) + '" IN (' + whereIds.join(', ') + ')' : null;

    }

    _computeWhereExists(type, options, meta, params) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const whereExists = [];

        for (const field in options.exists) {

            if (logicalOperators.indexOf(field) >= 0) {
                continue;
            }

            if (!recordTypes[type][field]) {
                continue;
            }

            const isArray = recordTypes[type][field][isArrayKey];
            const value = options.exists[field];

            if (!isArray) {
                whereExists.push('"' + this._computeTypeColumn(type, field) + '" ' + (value ? 'IS NOT NULL' : 'IS NULL'));
                continue;
            }

            whereExists.push('COALESCE(ARRAY_LENGTH("' + this._computeTypeColumn(type, field) + '", 1), 0) ' + (value ? '> 0' : '= 0'));

        }

        return whereExists.length ? this._computeWhereLogicalOperators(type, options.range, meta, params, whereExists.join(' AND ')) : null;

    }

    _computeWhereMatch(type, options, meta, params) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const whereMatch = [];

        for (const field in options.match) {

            if (logicalOperators.indexOf(field) >= 0) {
                continue;
            }

            if (!recordTypes[type][field]) {
                continue;
            }

            const isArray = recordTypes[type][field][isArrayKey];
            let values = options.match[field];

            if (!Array.isArray(values)) {
                values = [values];
            }

            const whereOr = [];

            if (!isArray) {

                const whereIn = [];

                values.map((value) => {

                    if(value !== null) {
                        params.push(value);
                        whereIn.push('$' + params.length);
                    }
                    else {
                        whereOr.push('"' + this._computeTypeColumn(type, field) + '" IS NULL');
                    }

                });

                if(whereIn.length) {
                    whereOr.push('"' + this._computeTypeColumn(type, field) + '" IN (' + whereIn.join(', ') + ')');
                }

            } else {

                if (!Array.isArray(values[0])) {
                    values = [values];
                }

                values.map((value) => {

                    const whereIn = [];

                    value.map((match) => {
                        params.push(match);
                        whereIn.push('$' + params.length + ' = ANY ("' + this._computeTypeColumn(type, field) + '")');
                    });

                    whereOr.push('(' + whereIn.join(' AND ') + ')');

                });

            }

            whereMatch.push('(' + whereOr.join(' OR ') + ')');

        }

        return whereMatch.length ? this._computeWhereLogicalOperators(type, options.match, meta, params, whereMatch.join(' AND ')) : null;

    }

    _computeWhereRange(type, options, meta, params) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const whereRange = [];

        for (const field in options.range) {

            if (logicalOperators.indexOf(field) >= 0) {
                continue;
            }

            if (!recordTypes[type][field]) {
                continue;
            }

            const isArray = recordTypes[type][field][isArrayKey];
            let values = options.range[field];

            if (!Array.isArray(values[0])) {
                values = [values];
            }

            const whereOr = [];

            values.map((value) => {

                const whereAnd = [];

                value.map((range, index) => {

                    if (range != null) {

                        params.push(range);

                        if (!isArray) {
                            whereAnd.push('"' + this._computeTypeColumn(type, field) + '" ' + (index ? '<' : '>') + '= $' + params.length);
                        } else {
                            whereAnd.push('COALESCE(ARRAY_LENGTH("' + this._computeTypeColumn(type, field) + '", 1), 0)  ' + (index ? '<' : '>') + '= $' + params.length);
                        }

                    }

                });

                whereOr.push('(' + whereAnd.join(' AND ') + ')');

            });

            whereRange.push('(' + whereOr.join(' OR ') + ')');

        }

        return whereRange.length ? this._computeWhereLogicalOperators(type, options.range, meta, params, whereRange.join(' AND ')) : null;

    }

    _computeWhereLogicalOperators(type, options, meta, params, where) {

        if (!where.length) {
            return null;
        }

        logicalOperators.map((logicalOperator) => {

            if (options[logicalOperator]) {
                where = [
                    where,
                    this._computeWhere(type, undefined, options[logicalOperator], meta, params, false)
                ].filter(Boolean);

                where = '(' + where.join(' ' + logicalOperator.toUpperCase() + ' ') + ')';
            }

        });

        return where;

    }

    create(type, records, meta) {

        if (!records.length) {
            return super.create();
        }

        const Promise = this.Promise;
        const client = this.client;
        const options = this.options;
        const recordTypes = this.recordTypes;

        const params = [];
        const values = [];

        const fields = Object.keys(recordTypes[type]);

        const typeId = this._computeTypeId(type);
        fields.unshift(typeId);

        records = records.map((record) => {

            const clone = Object.assign({
                id: this._computeId(type)
            }, record);

            record[typeId] = clone.id;

            let value = [];

            fields.map((field, index) => {
                params.push(record[field]);
                value.push('$' + params.length);
            });

            values.push('(' + value.join(', ') + ')');

            return clone;

        });

        const sql = 'INSERT INTO ' + this._computeTable(type) + '("' + fields.map(field => this._computeTypeColumn(type, field)).join('", "') + '") VALUES ' + values.join(', ');

        return client.query(sql, params)
            .then(() => {
                return records;
            });

    }

    delete(type, ids, meta) {

        // Handle no-op
        if (ids && !ids.length) {
            return super.delete();
        }

        const Promise = this.Promise;
        const client = this.client;

        const whereIn = ids.map((id, index) => {
            return '$' + (index + 1);
        });

        const sql = 'DELETE FROM ' + this._computeTable(type) + ' WHERE "' + this._computeTypeId(type) + '" IN (' + whereIn.join(', ') + ')';
        const params = ids;

        return client.query(sql, params)
            .then(() => {
                return ids.length;
            });

    }

    update(type, updates, meta) {

        // Handle no-op
        if (!updates.length) {
            return super.update();
        }

        const Promise = this.Promise;
        const client = this.client;

        const typeMap = this.options.typeMap || {};
        if (!typeMap[type]) {
            throw new Error('A typeMap is required for type "' + type + '".');
        }

        const recordTypes = this.recordTypes;
        const denormalizedInverseKey = this.keys.denormalizedInverse

        const typeId = this._computeTypeId(type);

        return Promise.all(updates.map((update, index) => {

                const params = [];
                const sets = [];

                if (update.operate !== undefined) {
                    throw new Error('Option operate for update not supported');
                }

                update.replace = update.replace || {};
                for (const field in update.replace) {

                    params.push(update.replace[field]);
                    sets.push('"' + this._computeTypeColumn(type, field) + '" = $' + params.length);

                };

                update.push = update.push || {};
                for (const field in update.push) {

                    if (denormalizedInverseKey in recordTypes[type][field]) {
                        return;
                    }

                    if (!Array.isArray(update.push[field])) {
                        update.push[field] = [update.push[field]];
                    }

                    params.push(update.push[field]);
                    const add = 'ARRAY_CAT("' + this._computeTypeColumn(type, field) + '", $' + params.length + ')';

                    params.push(update.id);
                    sets.push('"' + this._computeTypeColumn(type, field) + '" = (SELECT ARRAY(SELECT DISTINCT UNNEST(' + add + ')) FROM ' + this._computeTable(type) + ' WHERE "' + this._computeTypeId(type) + '" = $' + params.length + ')');

                }

                update.pull = update.pull || {};
                for (const field in update.pull) {

                    if (denormalizedInverseKey in recordTypes[type][field]) {
                        return;
                    }

                    if (!Array.isArray(update.pull[field])) {
                        update.pull[field] = [update.pull[field]];
                    }

                    if (update.pull[field].length) {

                        let remove = '"' + this._computeTypeColumn(type, field) + '"';

                        update.pull[field].forEach((value) => {
                            params.push(value);
                            remove = 'ARRAY_REMOVE(' + remove + ', $' + params.length + ')';
                        });

                        params.push(update.id);
                        sets.push('"' + this._computeTypeColumn(type, field) + '" = (SELECT ARRAY(SELECT DISTINCT UNNEST(' + remove + ')) FROM ' + this._computeTable(type) + ' WHERE "' + this._computeTypeId(type) + '" = $' + params.length + ')');

                    }

                }

                if (!sets.length) {
                    return null;
                }

                params.push(update.id);

                const sql = 'UPDATE ' + this._computeTable(type) + ' SET ' + sets.join(', ') + ' WHERE "' + this._computeTypeId(type) + '" = $' + params.length;

                return client.query(sql, params);

            }))
            .then((results) => {
                return results.filter(Boolean).length;
            });

    }

    find(type, ids, options, meta) {

        // Handle no-op
        if (ids && !ids.length) {
            return super.find();
        }

        options = options || {};

        if (options.query) {
            throw new Error('Option query for find not supported');
        }

        options.count = options.count || 'results';

        meta = meta || {};
        
        const Promise = this.Promise;
        const client = this.client;
        const isArrayKey = this.keys.isArray;

        const typeMap = this.options.typeMap || {};
        if (!typeMap[type]) {
            throw new Error('A typeMap is required for type "' + type + '".');
        }

        const selectSql = this._computeSelect(type, options);
        const fromSql = this._computeFrom(type, ids, options);

        const params = [];

        const whereSql = this._computeWhere(type, ids, options, meta, params);

        const sortSql = this._computeSort(type, options);
        const limitSql = this._computeLimit(options);
        const offsetSql = this._computeOffset(options);

        const records = client.query([
            selectSql,
            fromSql,
            whereSql,
            sortSql,
            limitSql,
            offsetSql
        ].filter(Boolean).join(' '), params)
            .then(results => results.rows);

        const count = options.total === 'rows' ? client.query([
            'SELECT COUNT(*)::integer as count',
            fromSql,
            whereSql,
        ].filter(Boolean).join(' '), params)
            .then(results => results.rows[0].count) : records.then(records => records.length);

        return Promise.all([
            records,
            count
        ]).then(([records, count]) => {
            records.count = count;
            return records;
        });

    }

    now() {

        const pool = this.pool();

        const sql = "SELECT TIMEZONE($1, NOW())::text AS now";
        const params = ['UTC'];

        return pool.query(sql, params)
            .then((results) => {
                return results.rows[0].now;
            });

    }

    beginTransaction() {

        const pool = this.pool();

        const transaction = Object.create(Object.getPrototypeOf(this))

        return pool.connect()
            .then((client) => {

                Object.assign(transaction, this, {
                    client,
                    endTransaction(error) {

                        const sql = error ? 'ROLLBACK' : 'COMMIT';
                        return client.query(sql)
                            .then(() => {
                                client.release();
                            });

                    }

                });

                const sql = "BEGIN";
                return client.query(sql)
                    .then((results) => {
                        return transaction;
                    });

            });

    }

}