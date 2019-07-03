const { gql } = require("apollo-server");

// Hard coded data here
const DATA = "";

export const schema = gql`
  extend type Query {
    foo: String
  }
`;

export const resolvers = {
  Query: {
    foo() {
      return DATA;
    }
  }
};
