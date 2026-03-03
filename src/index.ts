import { StelaDB } from "./database";
import { Query } from "./query";

const db = new StelaDB("./data");
const result = new Query("cars")
    .where("year", 2018)
    .groupBy("make")
    .select("price", "min")
    .select("price", "max")
    .select("mileage", "mean")
    .evaluate(db);

console.log(result);