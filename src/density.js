const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    density(
      dashboardID: String!
      label: AttributeInput!
      highlightedGroup: AttributeInput
    ): [DensityBin!]

    attributes(dashboardType: String!, dashboardID: String!): [Attribute!]!
  }

  type DensityBin {
    x: Float!
    y: Float!
    label: String!
    value: StringOrNum!
  }

  type Attribute {
    isNum: Boolean!
    type: String!
    label: String!
  }

  input AttributeInput {
    isNum: Boolean!
    type: String!
    label: String!
    value: StringOrNum
  }
`;

export const resolvers = {
  Query: {
    async density(_, { dashboardID, highlightedGroup, label }) {
      const sizeQuery = bodybuilder()
        .size(0)
        .filter("term", "dashboard_id", dashboardID)
        .aggregation("stats", "x")
        .aggregation("stats", "y")
        .build();

      const sizeResults = await client.search({
        index: "dashboard_cells",
        body: sizeQuery
      });

      const { agg_stats_x, agg_stats_y } = sizeResults["aggregations"];

      const xBinSize = (agg_stats_x["max"] - agg_stats_x["min"]) / 100;
      const yBinSize = (agg_stats_y["max"] - agg_stats_y["min"]) / 100;
      const data = await getBinnedData(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
      return data;
    },

    async attributes(_, { dashboardType, dashboardID }) {
      // PURPOSE: Get all possible coloring values for UMAP

      // Categorical values
      const CELL_CATEGORICAL = ["celltype"].map(label => ({
        isNum: false,
        type: "CELL",
        label
      }));

      const SAMPLE_CATEGORICAL = [
        "surgery",
        "site",
        "treatment"
      ].map(label => ({ label, isNum: false, type: "SAMPLE" }));

      // Numerical values
      const cellFields = await client.indices.getMapping({
        index: "dashboard_cells"
      });

      const CELL_NUMERICAL = Object.keys(
        cellFields["dashboard_cells"]["mappings"]["properties"]
      )
        .filter(
          field =>
            ![
              "dashboard_id",
              "cell_id",
              "cell_type",
              "x",
              "y",
              "sample_id"
            ].includes(field)
        )
        .map(label => ({ isNum: true, label, type: "CELL" }));

      const geneQuery = bodybuilder()
        .size(0)
        .agg("terms", "gene", { size: 50000, order: { _key: "asc" } })
        .build();

      const geneResults = await client.search({
        index: `dashboard_genes_${dashboardID.toLowerCase()}`,
        body: geneQuery
      });

      const GENE_NUMERICAL = geneResults["aggregations"]["agg_terms_gene"][
        "buckets"
      ].map(bucket => ({ isNum: true, label: bucket["key"], type: "GENE" }));

      return [
        ...CELL_CATEGORICAL,
        ...(dashboardType === "SAMPLE" ? [] : SAMPLE_CATEGORICAL),
        ...CELL_NUMERICAL,
        ...GENE_NUMERICAL
      ];
    }
  }
};

async function getBinnedData(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  if (label["isNum"]) {
    if (label["type"] === "CELL") {
      return await getCellNumericalBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
    } else {
      // is "GENE"
      return await getGeneBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
    }
  } else {
    // is categorical
    if (label["type"] === "CELL") {
      return await getCelltypeBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
    } else {
      // is "SAMPLE"
      return await getSampleBins(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
    }
  }
}

async function getCelltypeBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  if (!highlightedGroup) {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "cell_type", { size: 1000 })
    ).build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          label["label"],
          yBucket => yBucket["agg_terms_cell_type"]["buckets"][0]["key"]
        )
      ],
      []
    );
  } else if (isSameLabel(label, highlightedGroup)) {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "cell_type", { size: 1000 })
    ).build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          highlightedGroup["value"],
          yBucket =>
            calculateProportion(
              yBucket["agg_terms_cell_type"]["buckets"],
              highlightedGroup["value"]
            )
        )
      ],
      []
    );
  } else {
    // highlightedGroup is another filter

    const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);

    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "cell_type", { size: 1000 })
    )
      .filter("terms", "cell_id", cellIDs)
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: bucket["agg_terms_cell_type"][
            "buckets"
          ][0]["key"]
        }),
        {}
      );

    const dataBins = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );

    return allBins.map(record => ({
      ...record,
      value:
        dataBins.hasOwnProperty(record["x"]) &&
        dataBins[record["x"]].hasOwnProperty(record["y"])
          ? dataBins[record["x"]][record["y"]]
          : ""
    }));
  }
}

async function getSampleBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  const [patientID, sortID] = dashboardID.split("_");

  const sampleIDquery = bodybuilder()
    .size(1000)
    .filter("term", "patient_id", patientID)
    .filter("term", "sort", sortID)
    .filter("term", "type", "sample")
    .build();

  const sampleIDresults = await client.search({
    index: "dashboard_entry",
    body: sampleIDquery
  });

  const sampleIDMap = sampleIDresults["hits"]["hits"]
    .map(record => record["_source"])
    .reduce(
      (sampleMap, record) => ({
        ...sampleMap,
        [record["dashboard_id"]]: record[label["label"]][0]
      }),
      {}
    );

  if (!highlightedGroup) {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "sample_id", { size: 1000 })
    ).build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          label["label"],
          yBucket =>
            sampleIDMap[yBucket["agg_terms_sample_id"]["buckets"][0]["key"]]
        )
      ],
      []
    );
  } else if (isSameLabel(label, highlightedGroup)) {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "sample_id", { size: 1000 })
    ).build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          highlightedGroup["value"],
          yBucket =>
            calculateProportion(
              yBucket["agg_terms_sample_id"]["buckets"].map(record => ({
                ...record,
                key: sampleIDMap[record["key"]]
              })),
              highlightedGroup["value"]
            )
        )
      ],
      []
    );
  } else {
    // highlightedGroup is another filter

    const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);

    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("terms", "sample_id", { size: 1000 })
    )
      .filter("terms", "cell_id", cellIDs)
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: sampleIDMap[
            bucket["agg_terms_sample_id"]["buckets"][0]["key"]
          ]
        }),
        {}
      );

    const dataBins = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );

    return allBins.map(record => ({
      ...record,
      label: label["label"],
      value:
        dataBins.hasOwnProperty(record["x"]) &&
        dataBins[record["x"]].hasOwnProperty(record["y"])
          ? dataBins[record["x"]][record["y"]]
          : ""
    }));
  }
}

async function getGeneBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  if (!highlightedGroup) {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("percentiles", "log_count")
    )
      .filter("term", "gene", label["label"])
      .build();

    const results = await client.search({
      index: `dashboard_genes_${dashboardID.toLowerCase()}`,
      body: query
    });
    return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          label["label"],
          yBucket => yBucket[`agg_percentiles_log_count`]["values"]["50.0"]
        )
      ],
      []
    );
  } else if (isSameLabel(label, highlightedGroup)) {
    const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("percentiles", "log_count")
    )
      .filter("term", "gene", label["label"])
      .filter("range", "log_count", {
        gte: highlightedGroup["value"].split("-")[0].trim(),
        lt: highlightedGroup["value"].split("-")[1].trim()
      })
      .build();

    const results = await client.search({
      index: `dashboard_genes_${dashboardID.toLowerCase()}`,
      body: query
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: bucket["doc_count"]
        }),
        {}
      );

    const dataBins = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );

    return allBins.map(record => ({
      ...record,
      value:
        dataBins.hasOwnProperty(record["x"]) &&
        dataBins[record["x"]].hasOwnProperty(record["y"])
          ? dataBins[record["x"]][record["y"]] / record["value"]
          : ""
    }));
  } else {
    // highlightedGroup is another filter

    const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);

    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("percentiles", "log_count")
    )
      .filter("term", "gene", label["label"])
      .filter("terms", "cell_id", cellIDs)
      .build();

    const results = await client.search({
      index: `dashboard_genes_${dashboardID.toLowerCase()}`,
      body: query
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: bucket[
            `agg_percentiles_log_count`
          ]["values"]["50.0"]
        }),
        {}
      );

    const dataBins = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );

    return allBins.map(record => ({
      ...record,
      label: label["label"],
      value:
        dataBins.hasOwnProperty(record["x"]) &&
        dataBins[record["x"]].hasOwnProperty(record["y"])
          ? dataBins[record["x"]][record["y"]]
          : ""
    }));
  }
}

async function getCellNumericalBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  if (!highlightedGroup) {
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("percentiles", label["label"])
    ).build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });
    return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
      (records, xBucket) => [
        ...records,
        ...processXBuckets(
          xBucket,
          xBinSize,
          yBinSize,
          label["label"],
          yBucket =>
            yBucket[`agg_percentiles_${label["label"]}`]["values"]["50.0"]
        )
      ],
      []
    );
  } else if (isSameLabel(label, highlightedGroup)) {
    const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);
    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("percentiles", label["label"])
    )
      .filter("range", highlightedGroup["label"], {
        gte: highlightedGroup["value"].split("-")[0].trim(),
        lt: highlightedGroup["value"].split("-")[1].trim()
      })
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: bucket["doc_count"]
        }),
        {}
      );

    const dataBins = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );

    return allBins.map(record => ({
      ...record,
      value:
        dataBins.hasOwnProperty(record["x"]) &&
        dataBins[record["x"]].hasOwnProperty(record["y"])
          ? dataBins[record["x"]][record["y"]] / record["value"]
          : ""
    }));
  } else {
    // highlightedGroup is another filter

    const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

    const cellIDs = await getCellIDs(dashboardID, highlightedGroup);

    const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
      a.aggregation("percentiles", label["label"])
    )
      .filter("terms", "cell_id", cellIDs)
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    const processProbYBucket = yBuckets =>
      yBuckets.reduce(
        (yMap, bucket) => ({
          ...yMap,
          [Math.round(bucket["key"] / yBinSize)]: bucket[
            `agg_percentiles_${label["label"]}`
          ]["values"]["50.0"]
        }),
        {}
      );

    const dataBins = results["aggregations"]["agg_histogram_x"][
      "buckets"
    ].reduce(
      (xMap, xBucket) => ({
        ...xMap,
        [Math.round(xBucket["key"] / xBinSize)]: processProbYBucket(
          xBucket["agg_histogram_y"]["buckets"]
        )
      }),
      {}
    );

    return allBins.map(record => ({
      ...record,
      label: label["label"],
      value:
        dataBins.hasOwnProperty(record["x"]) &&
        dataBins[record["x"]].hasOwnProperty(record["y"])
          ? dataBins[record["x"]][record["y"]]
          : ""
    }));
  }
}

const isSameLabel = (label, highlightedGroup) =>
  label["label"] === highlightedGroup["label"];

async function getCellIDs(dashboardID, highlightedGroup) {
  if (highlightedGroup["type"] === "CELL") {
    const baseQuery = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID);

    const query = highlightedGroup["isNum"]
      ? baseQuery
          .filter("range", highlightedGroup["label"], {
            gte: highlightedGroup["value"].split("-")[0].trim(),
            lt: highlightedGroup["value"].split("-")[1].trim()
          })
          .build()
      : baseQuery
          .filter("term", highlightedGroup["label"], highlightedGroup["value"])
          .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
  } else if (highlightedGroup["type"] === "SAMPLE") {
    const sampleIDQuery = bodybuilder()
      .size(1000)
      .filter("term", "patient_id", dashboardID.split("_")[0])
      .filter("term", "sort", dashboardID.split("_")[1])
      .filter("term", highlightedGroup["label"], highlightedGroup["value"])
      .filter("term", "type", "sample")
      .build();

    const sampleIDResults = await client.search({
      index: "dashboard_entry",
      body: sampleIDQuery
    });

    const sampleIDs = sampleIDResults["hits"]["hits"].reduce(
      (sampleIDs, record) => [...sampleIDs, ...record["_source"]["sample_ids"]],
      []
    );

    const query = bodybuilder()
      .size(50000)
      .filter("term", "dashboard_id", dashboardID)
      .filter("terms", "sample_id", sampleIDs)
      .build();

    const results = await client.search({
      index: "dashboard_cells",
      body: query
    });

    return results["hits"]["hits"].map(record => record["_source"]["cell_id"]);
  } else {
    // is "GENE"
    const query = bodybuilder()
      .size(0)
      .filter("term", "gene", highlightedGroup["label"])
      .filter("range", "log_count", {
        gte: highlightedGroup["value"].split("-")[0].trim(),
        lt: highlightedGroup["value"].split("-")[1].trim()
      })
      .aggregation("terms", "cell_id", { size: 50000 })
      .build();

    const results = await client.search({
      index: `dashboard_genes_${dashboardID.toLowerCase()}`,
      body: query
    });

    return results["aggregations"]["agg_terms_cell_id"]["buckets"].map(
      bucket => bucket["key"]
    );
  }
}

async function getAllBins(dashboardID, xBinSize, yBinSize) {
  const query = getBaseDensityQuery(dashboardID, xBinSize, yBinSize, a =>
    a.aggregation("terms", "cell_type", { size: 1000 })
  );
  const results = await client.search({
    index: "dashboard_cells",
    body: query.build()
  });

  return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
    (records, xBucket) => [
      ...records,
      ...processXBuckets(
        xBucket,
        xBinSize,
        yBinSize,
        "total",
        yBucket => yBucket["doc_count"]
      )
    ],
    []
  );
}

const getBaseDensityQuery = (dashboardID, xBinSize, yBinSize, labelAgg) =>
  bodybuilder()
    .size(0)
    .filter("term", "dashboard_id", dashboardID)
    .aggregation(
      "histogram",
      "x",
      { interval: xBinSize, min_doc_count: 1 },
      a =>
        a.aggregation(
          "histogram",
          "y",
          { interval: yBinSize, min_doc_count: 1 },
          labelAgg
        )
    );

const processXBuckets = (xBucket, xBinSize, yBinSize, label, getValue) =>
  xBucket["agg_histogram_y"]["buckets"].map(yBucket => {
    return {
      x: Math.round(xBucket["key"] / xBinSize),
      y: Math.round(yBucket["key"] / yBinSize),
      value: getValue(yBucket),
      label
    };
  });

const calculateProportion = (counts, highlightedGroup) => {
  const total = counts.reduce(
    (currSum, record) => currSum + record["doc_count"],
    0
  );

  const filteredRecords = counts.filter(
    record => record["key"] === highlightedGroup
  );

  return filteredRecords.length === 0
    ? 0
    : filteredRecords[0]["doc_count"] / total;
};
