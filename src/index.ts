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
