import "@babel/polyfill";

import { ApolloServer } from "apollo-server-express";
import { gql } from "apollo-server";

import * as cells from "./cells";
import * as samples from "./samples";

import { merge } from "lodash";

const baseSchema = gql`
  type Query {
    _blank: String
  }
`;

const server = new ApolloServer({
  typeDefs: [baseSchema, cells.schema, samples.schema],
  resolvers: merge(cells.resolvers, samples.resolvers)
});

const express = require("express");
const app = express();
server.applyMiddleware({ app });

app.listen({ port: 4000 }, () =>
  console.log(`🚀 Server ready at http://localhost:4000${server.graphqlPath}`)
);
