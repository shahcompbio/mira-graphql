const elasticsearch = require("elasticsearch");

const HOST = process.env.HOST || "localhost";
const client = new elasticsearch.Client({
  host: HOST + ":" + "9200"
});

export default client;
