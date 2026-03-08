import * as Aggregation from "../aggregation";
import { Column } from "./columns";

/**
 * A column that is stored dictionary encoded, i.e. 
 * the first line is a serialized array of all unique values
 * in the column. Each subsequent line is an index into that array, 
 * indicating the value for that row. 
 */
export class DictionaryColumn<T> implements Column {
    static format: string = "dictionary";
    private _key: unknown[];
    private _items: number[];

    constructor(content: string) {
        this._key = JSON.parse(content.substring(0, content.indexOf("\n")));
        this._items = content.split("\n").slice(1).filter(l => l.length > 0).map(l => parseInt(l));
    }

    filter(value: T): Set<number> {
        return new Set(this._items.flatMap((v, ix) => this.get(ix) === value ? [ix] : []));
    }

    group(mask: Set<number> = new Set([...Array(this._items.length).keys()])): Map<unknown, Set<number>> {
        const groups = new Map<unknown, Set<number>>();

        for (let row_ix = 0; row_ix < this._items.length; row_ix++) {
            if (!mask.has(row_ix)) {
                continue;
            }

            const v = this.get(row_ix);
            if (!groups.has(v)) {
                groups.set(v, new Set());
            }
            const group = groups.get(v)!;
            groups.set(v, group.union(new Set([row_ix])));
        }

        return groups;
    }

    aggregate(aggregator: Aggregation.Aggregator, mask: Set<number>): number {
        const masked = [...mask].map((row_ix) => this.get(row_ix));
        return aggregator.aggregate(masked);
    }

    get(row_ix: number): unknown {
        return this._key[this._items[row_ix]];
    }

    length(): number {
        return this._items.length;
    }
}
