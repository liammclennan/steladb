import StelaDB from "./index";
import { Query } from "./query";

const db = new StelaDB("./data");

const queries = [
    new Query("digits").select("value", "max"),
];

const start = performance.now();

for (const query of queries) {
    console.log(query.toString());
    console.table(db.evaluate(query));
}

const end = performance.now();
console.log(`Execution time: ${end - start} ms`);