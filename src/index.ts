import { StelaDB } from "./database";
import { Query } from "./query";

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
    console.log(query.evaluate(db));
}