interface Column {
    filter(value: unknown):  Set<number>;
    group(): Map<unknown, Set<number>>;
}

interface Table {
    columns: Map<string, Column>;
}

class LiteralColumn<T> implements Column {
    _data: unknown[];

    constructor(data: T[]) {
        this._data = data;
    }

    filter(value: T): Set<number> {
        return new Set(this._data.flatMap((v, ix) => v === value ? [ix] : []));
    }

    group(mask: Set<number> = new Set([...Array(this._data.length).keys()])): Map<unknown, Set<number>> {
        const groups = new Map<unknown, Set<number>>();
        
        for (let row_ix = 0; row_ix < this._data.length; row_ix++) {
            if (!mask.has(row_ix)) { 
                continue;
            }

            const v = this._data[row_ix];
            const group = groups.getOrInsert(v, new Set());
            groups.set(v, group.union(new Set([row_ix])));
        }

        return groups;
    }
}

function columnFrom(data: unknown[]): Column {
    return new LiteralColumn(data);
}

const makes = columnFrom(['Honda', 'Tesla', 'Honda','Ford','BMW','Ford','Volkswagen','Tesla', 'Ford', 'Kia']);

// @ts-ignore: typescript typings for Set constructor are wrong
console.log(makes.group(new Set([5,6,7,8,9])));

type Aggregation = "min" | "max" | "sum";

class Database {
    private _path: string;

    constructor(path: string) {
        this._path = path;
    }

    query(table: string) {
        return new Query(table, this);
    }

    column(table: string, columnName: string): Column {
        return new LiteralColumn([]);
    }
}

class Query {
    private _database: Database;
    private _table: string;

    private _aggregations: { columnName: string, aggregation: Aggregation }[] = [];
    private _filters: { columnName: string, value: unknown }[] = [];
    private _grouping: string | undefined;

    constructor(table: string, database: Database) {
        this._table = table;
        this._database = database;
    }

    select(columnName: string, aggregation: Aggregation): Query {
        this._aggregations.push({ columnName, aggregation });
        return this;
    }

    where(columnName: string, value: unknown) {
        this._filters.push({ columnName, value });
        return this;
    }

    groupBy(columnName: string) {
        this._grouping = columnName;
        return this;
    }

    evaluate() {
        const restriction = this._filters.reduce((p, c) => {
            const col = this._database.column(this._table, c.columnName);
            return p.intersection(col.filter(c.value));            
        }, new Set());
    }
}

const db = new Database("/home/liam/code/colstore");
const query = db.query("cars")
    .where("year", 1999)
    .groupBy("make")
    .select("price", "min");