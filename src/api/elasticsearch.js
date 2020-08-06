
require("dotenv").config();
const elasticsearch = require("elasticsearch");

const client = new elasticsearch.Client({
  host: process.env.HOST || process.env.DB_HOST || "localhost:9200"
});

export default client;
