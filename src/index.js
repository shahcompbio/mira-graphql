import "@babel/polyfill";

import { ApolloServer } from "apollo-server-express";
import { gql } from "apollo-server";

import * as foo from "./foo.js";

import { merge } from "lodash";

const baseSchema = gql`
  type Query {
    _blank: String
  }
`;

const server = new ApolloServer({
  typeDefs: [baseSchema, foo.schema],
  resolvers: merge(foo.resolvers)
});

const express = require("express");
const app = express();
server.applyMiddleware({ app });

app.listen({ port: 4000 }, () =>
  console.log(`🚀 Server ready at http://localhost:4000${server.graphqlPath}`)
);
