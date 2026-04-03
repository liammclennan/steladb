import { Query } from "./query";
import * as Evaluation from "./evaluation/evaluation";

export default class StelaDB {
    private _path: string;

    constructor(path: string) {
        this._path = path;
    }

    evaluate(query: Query) {
        return Evaluation.evaluate(this._path, query);
    }
}

export function dump(table: Evaluation.Row[]) {
    const headers = table[0];

    console.table(
        table.slice(1)
            .map(row => Object.fromEntries(
                headers.map((header,ix) => [header, row[ix]])
            )));
}