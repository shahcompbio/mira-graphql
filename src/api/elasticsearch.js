const elasticsearch = require("elasticsearch");

const HOST = process.env.HOST || "localhost:9200";
const client = new elasticsearch.Client({
  host: HOST
});

export default client;
