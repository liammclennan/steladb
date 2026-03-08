import { Query } from "./query";
import * as Evaluation from "./evaluation";

export class StelaDB {
    private _path: string;

    constructor(path: string) {
        this._path = path;
    }

    evaluate(query: Query) {
        return Evaluation.evaluate(this._path, query);
    }
}

const db = new StelaDB("./data");

const queries = [

    new Query("cars")
        .where("year", 2018)
        .groupBy("make")
        .select("price", "min")
        .select("price", "max")
        .select("mileage", "mean"),

    new Query("cars")
        .select("*"),

    new Query("cars")
        .select("year")
        .select("model")
        .select("mileage")

];

for (const query of queries) {
    console.log(query.toString());
    console.table(db.evaluate(query));
}