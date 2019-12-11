const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";
import getSampleIDs from "./utils/getSampleIDs.js";

export const schema = gql`
  extend type Query {
    cells(
      type: String!
      dashboardID: String!
      props: [DashboardAttributeInput!]!
    ): [Cell!]!
    dashboardCellAttributes(
      type: String!
      dashboardID: String!
    ): [DashboardAttribute!]!
    dashboardAttributeValues(
      type: String!
      dashboardID: String!
      prop: DashboardAttributeInput!
    ): [DashboardAttributeValue!]!
  }

  type Cell {
    id: ID
    name: String
    x: Float
    y: Float
    celltype: String
    values: [CellAttribute]
  }

  type CellAttribute {
    label: String
    value: StringOrNum
  }

  type DashboardAttribute {
    label: String!
    type: String!
  }

  type DashboardAttributeValue {
    label: StringOrNum!
    count: Int
  }

  input DashboardAttributeInput {
    label: String!
    type: String!
  }
`;

export const resolvers = {
  Query: {
    async cells(_, { type, dashboardID, props }) {
      const sampleIDs = await getSampleIDs(type, dashboardID);

      const { geneProps, cellProps, sampleProps } = separateProps(props);

      // Get redim
      const query = bodybuilder()
        .size(50000)
        .filter("term", "dashboard_id", dashboardID)
        .build();

      const results = await client.search({
        index: `dashboard_cells`,
        body: query
      });

      // get sample cells; then cross filter

      // Get metadata
      // Get cells props (+ celltype)
      // Get gene props

      const geneMap =
        geneProps.length > 0 ? await getGeneMap(dashboardID, geneProps) : {};

      const sampleMap =
        sampleProps.length > 0 ? await getSampleMap(sampleIDs) : {};

      return results["hits"]["hits"].map(record => ({
        ...record["_source"],
        cellProps,
        genes: [geneProps, geneMap],
        samples: [sampleProps, sampleMap]
      }));
    },

    async dashboardCellAttributes(_, { type, dashboardID }) {
      const cellFields = await client.indices.getMapping({
        index: "dashboard_cells"
      });

      const cellAttributes = Object.keys(
        cellFields["dashboard_cells"]["mappings"]["properties"]
      )
        .filter(
          field =>
            !["dashboard_id", "cell_id", "cell_type", "x", "y"].includes(field)
        )
        .map(field => ({ label: field, type: "CELL" }));

      const geneQuery = bodybuilder()
        .size(0)
        .agg("terms", "gene", { size: 50000, order: { _key: "asc" } })
        .build();

      const geneResults = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: geneQuery
      });

      const geneAttributes = geneResults["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => ({ label: bucket["key"], type: "GENE" }));

      const sampleAttributes =
        type === "sample"
          ? []
          : ["surgery", "site", "treatment"].map(attr => ({
              label: attr,
              type: "SAMPLE"
            }));

      return [
        { label: "celltype", type: "CELL" },
        ...sampleAttributes,
        ...cellAttributes,
        ...geneAttributes
      ];
    },

    async dashboardAttributeValues(_, { type, dashboardID, prop }) {
      if (prop["label"] === "celltype") {
        const query = bodybuilder()
          .size(0)
          .agg("terms", "celltype", { size: 50 }, a => {
            return a.aggregation("terms", "marker", { size: 50 });
          })
          .build();

        const results = await client.search({
          index: "rho_markers",
          body: query
        });

        return results["aggregations"]["agg_terms_celltype"][
          "buckets"
        ].sort((a, b) => (a["key"] < b["key"] ? -1 : 1));
      } else if (prop["type"] === "CELL") {
        const query = bodybuilder()
          .size(0)
          .filter("term", "dashboard_id", dashboardID)
          .agg(
            "histogram",
            prop["label"],
            { interval: 0.1, extended_bounds: { min: 0, max: 1 } },
            "agg_histogram"
          )
          .build();

        const results = await client.search({
          index: "dashboard_cells",
          body: query
        });

        return results["aggregations"][`agg_histogram`]["buckets"];
      } else if (prop["type"] === "SAMPLE") {
        const query = bodybuilder()
          .filter("term", "dashboard_id", dashboardID)
          .build();

        const results = await client.search({
          index: "dashboard_entry",
          body: query
        });
        // TODO: Currently doesn't take into account count (although none of the queries actually use this right now)
        return results["hits"]["hits"][0]["_source"][prop["label"]].map(
          label => ({
            key: label
          })
        );
      } else {
        const query = bodybuilder()
          .size(0)
          .filter("term", "gene", prop["label"])
          .agg("histogram", "log_count", { interval: 1 })
          .build();

        const results = await client.search({
          index: `dashboard_genes_${dashboardID.toLowerCase()}`,
          body: query
        });

        return results["aggregations"]["agg_histogram_log_count"]["buckets"];
      }
    }
  },

  Cell: {
    id: root => root["cell_id"],
    name: root => root["cell_id"],
    x: root => root["x"],
    y: root => root["y"],
    celltype: root => root["cell_type"],
    values: root => [
      ...root["cellProps"].map(prop => ({
        label: prop,
        value: root[prop]
      })),
      ...root["genes"][0].map(prop => ({
        label: prop,
        value:
          root["genes"][1].hasOwnProperty(root["cell_id"]) &&
          root["genes"][1][root["cell_id"]].hasOwnProperty(prop)
            ? root["genes"][1][root["cell_id"]][prop]["log_count"]
            : 0
      })),
      ...root["samples"][0].map(prop => {
        // console.log(
        //   root["samples"][1],
        //   root["cells"][1][root["cell_id"]]["sample_id"]
        // );

        return {
          label: prop,
          // Ugg this is awful but basically need to map by sampleID of cell
          value:
            root["samples"][1][root["cells"][1][root["cell_id"]]["sample_id"]][
              prop
            ][0]
        };
      })
    ]
  },

  CellAttribute: {
    label: root => root["label"],
    value: root => root["value"]
  },

  DashboardAttribute: {
    label: root => root["label"],
    type: root => root["type"]
  },

  DashboardAttributeValue: {
    label: root => root["key"],
    count: root => root["doc_count"]
  }
};

const separateProps = props =>
  props
    .filter(prop => prop["label"] !== "celltype")
    .reduce(
      (propMap, prop) =>
        prop["type"] === "GENE"
          ? { ...propMap, geneProps: [...propMap["geneProps"], prop["label"]] }
          : prop["type"] === "CELL"
          ? { ...propMap, cellProps: [...propMap["cellProps"], prop["label"]] }
          : {
              ...propMap,
              sampleProps: [...propMap["sampleProps"], prop["label"]]
            },
      { geneProps: [], cellProps: [], sampleProps: [] }
    );

async function getGeneMap(dashboardID, props) {
  const query = bodybuilder()
    .size(50000)
    .filter("term", "dashboard_id", dashboardID)
    .filter("terms", "gene", props)
    .build();

  const results = await client.search({
    index: `dashboard_genes_${dashboardID.toLowerCase()}`,
    body: query
  });

  const mapping = results["hits"]["hits"]
    .map(record => record["_source"])
    .reduce((geneMap, record) => {
      const cellID = record["cell_id"];
      if (geneMap.hasOwnProperty(cellID)) {
        const cellRecords = geneMap[cellID];

        return {
          ...geneMap,
          [cellID]: { ...cellRecords, [record["gene"]]: record }
        };
      } else {
        return { ...geneMap, [cellID]: { [record["gene"]]: record } };
      }
    }, {});

  return mapping;
}

async function getSampleMap(sampleIDs) {
  const query = bodybuilder()
    .size(1000)
    .filter("terms", "dashboard_id", sampleIDs)
    .build();

  const results = await client.search({
    index: "dashboard_entry",
    body: query
  });

  const mapping = results["hits"]["hits"]
    .map(record => record["_source"])
    .reduce(
      (currMap, record) => ({
        ...currMap,
        [record["dashboard_id"]]: record
      }),
      {}
    );

  return mapping;
}
