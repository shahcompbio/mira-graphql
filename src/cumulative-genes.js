const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    cumulativeGenes(dashboardID: String!, genes: [String!]!): [DensityBin!]!
  }
`;

export const resolvers = {
  Query: {
    async cumulativeGenes(_, { dashboardID, genes }) {
      return [];
    }
  }
};
