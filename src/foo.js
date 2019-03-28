const { gql } = require("apollo-server");

export const schema = gql`
  extend type Query {
    foo: [Bar!]
  }

  type Bar {
    id: String!
    name: String!
    age: Int!
  }
`;

export const resolvers = {
  Query: {
    foo() {
      return DATA;
    }
  },
  Bar: {
    id: root => root.id,
    name: root => root.name,
    age: root => root.age
  }
};

const DATA = [
  {
    id: "foo1",
    name: "Sam",
    age: 5
  },
  {
    id: "abc",
    name: "ABC",
    age: 100
  }
];
