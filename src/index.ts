import { StelaDB } from "./database";
import { Query } from "./query";

const db = new StelaDB("/home/liam/code/colstore");
const result = new Query("cars")
    .where("year", "1999")
    .groupBy("make")
    .select("price", "min")
    .select("price", "max")
    .select("price", "sum")
    .evaluate(db);

console.log(result);