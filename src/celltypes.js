const { gql } = require("apollo-server");
import { data } from "./data.js";

export const schema = gql`
  extend type Query {
    markers: [dataPair!]!
  }

  type dataPair {
    cellType: String!
    markerGenes: [String!]!
  }
`;

export const resolvers = {
  Query: {
    async markers() {
      return data;
    }
  }
};
