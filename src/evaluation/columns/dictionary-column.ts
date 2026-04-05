import * as Aggregation from "../aggregation";
import { Column } from "./columns";

/**
 * A column that is stored dictionary encoded, i.e. 
 * the first line is a serialized array of all unique values
 * in the column. Each subsequent line is an index into that array, 
 * indicating the value for that row. 
 * 
 * E.g. (make.txt)
 * 
 * ```md
 * dictionary
 * ["Toyota","Honda","Ford"]
 * 0
 * 1
 * 2
 * 2
 * 1
 * 1
 * ```
 */
export class DictionaryColumn<T> implements Column {
    static format: string = "dictionary";
    private _dictionary: unknown[];
    private _items: number[];

    constructor(content: string) {
        const [dictionary, ...values] = content.split("\n");
        this._dictionary = JSON.parse(dictionary);
        this._items = values
            .filter(l => l.length > 0)
            .map(l => parseInt(l, 10));
    }

    get(row_ix: number): unknown {
        return this._dictionary[this._items[row_ix]];
    }

    filter(value: T): Set<number> {
        const indexOfValue = this._dictionary.indexOf(value);
        
        return indexOfValue === null 
            ? new Set() 
            : new Set(this._items.flatMap((v, ix) => v === indexOfValue ? [ix] : []));
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
            groups.set(v, group.add(row_ix));
        }

        return groups;
    }

    aggregate(aggregator: Aggregation.Aggregator, mask: Set<number>): number {
        const masked = [...mask].map((row_ix) => this.get(row_ix));
        return aggregator.aggregate(masked);
    }

    length(): number {
        return this._items.length;
    }
}
