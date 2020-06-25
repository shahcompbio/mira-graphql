const { gql } = require("apollo-server");
import bodybuilder from "bodybuilder";

const util = require("util");
import client from "./api/elasticsearch.js";

export const schema = gql`
  extend type Query {
    density(
      dashboardID: String!
      label: AttributeInput!
      highlightedGroup: AttributeInput
    ): [DensityBin!]
  }

  type DensityBin {
    x: Float!
    y: Float!
    label: String!
    value: StringOrNum!
  }
`;

export const resolvers = {
  Query: {
    async density(_, { dashboardID, highlightedGroup, label }) {
      const { xBinSize, yBinSize } = await getBinSizes(dashboardID);

      const data = await getBinnedData(
        dashboardID,
        label,
        highlightedGroup,
        xBinSize,
        yBinSize
      );
      return data;
    },
  },
};

async function getBinnedData(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  if (label["isNum"]) {
    // is gene

    const dataMap = await getGeneBins(
      dashboardID,
      label,
      highlightedGroup,
      xBinSize,
      yBinSize
    );

    const records = await getRecords(
      dataMap,
      dashboardID,
      xBinSize,
      yBinSize,
      isSameLabel(label, highlightedGroup) ? "density" : label["label"],
      true
    );
    return records;
  } else {
    const dataMap = await getCategoricalBins(
      dashboardID,
      label,
      highlightedGroup,
      xBinSize,
      yBinSize
    );

    const records = await getRecords(
      dataMap,
      dashboardID,
      xBinSize,
      yBinSize,
      isSameLabel(label, highlightedGroup) ? "density" : label["label"],
      isSameLabel(label, highlightedGroup)
    );
    return records;
  }
}

export async function getRecords(
  dataMap,
  dashboardID,
  xBinSize,
  yBinSize,
  label,
  isDensity
) {
  const allBins = await getAllBins(dashboardID, xBinSize, yBinSize);

  return allBins.map((record) => {
    const { x, y, value } = record;

    return {
      x,
      y,
      label,
      value:
        dataMap.hasOwnProperty(x) && dataMap[x].hasOwnProperty(y)
          ? isDensity
            ? dataMap[x][y] / value
            : dataMap[x][y]
          : "",
    };
  });
}

export async function getAllBins(dashboardID, xBinSize, yBinSize) {
  const query = getBaseDensityQuery(xBinSize, yBinSize, (a) =>
    a.aggregation("terms", "cell_type", { size: 1000 })
  );
  const results = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: query.build(),
  });

  return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
    (records, xBucket) => [
      ...records,
      ...processXBuckets(
        xBucket,
        xBinSize,
        yBinSize,
        "total",
        (yBucket) => yBucket["doc_count"]
      ),
    ],
    []
  );
}

export const getBaseDensityQuery = (xBinSize, yBinSize, labelAgg) =>
  bodybuilder()
    .size(0)
    .aggregation(
      "histogram",
      "x",
      { interval: xBinSize, min_doc_count: 1 },
      (a) =>
        a.aggregation(
          "histogram",
          "y",
          { interval: yBinSize, min_doc_count: 1 },
          labelAgg
        )
    );

const processXBuckets = (xBucket, xBinSize, yBinSize, label, getValue) =>
  xBucket["agg_histogram_y"]["buckets"].map((yBucket) => {
    return {
      x: Math.round(xBucket["key"] / xBinSize),
      y: Math.round(yBucket["key"] / yBinSize),
      value: getValue(yBucket),
      label,
    };
  });

async function getCategoricalBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  // Query fetching
  const baseQuery = getBaseDensityQuery(xBinSize, yBinSize, (a) =>
    a.aggregation("terms", label["label"], { size: 1000 })
  );

  const query = highlightedGroup
    ? addFilter(baseQuery, highlightedGroup)
    : baseQuery;

  // console.log(
  //   util.inspect(query.build(), false, null, true /* enable colors */)
  // );
  const results = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: query.build(),
  });

  const getValue = isSameLabel(label, highlightedGroup)
    ? (bucket) => bucket["doc_count"]
    : (bucket) => bucket[`agg_terms_${label["label"]}`]["buckets"][0]["key"];

  return getDataMap(results, xBinSize, yBinSize, getValue);
}

async function getGeneBins(
  dashboardID,
  label,
  highlightedGroup,
  xBinSize,
  yBinSize
) {
  // Query fetching
  const baseQuery = getBaseDensityQuery(xBinSize, yBinSize, (a) =>
    a.agg("nested", { path: "genes" }, "agg_genes", (a) =>
      a.agg(
        "filter",
        { term: { "genes.gene": label["label"] } },
        "agg_gene_filter",
        (a) => a.agg("stats", "genes.log_count")
      )
    )
  );

  const query = highlightedGroup
    ? addFilter(baseQuery, highlightedGroup)
    : baseQuery;

  const results = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: query.build(),
  });

  // console.log(
  //   util.inspect(query.build(), false, null, true /* enable colors */)
  // );
  const getValue = isSameLabel(label, highlightedGroup)
    ? (bucket) => bucket["doc_count"]
    : (bucket) =>
        bucket["agg_genes"]["agg_gene_filter"]["agg_stats_genes.log_count"][
          "sum"
        ];

  return getDataMap(results, xBinSize, yBinSize, getValue);
}

const isSameLabel = (label, highlightedGroup) =>
  highlightedGroup && label["label"] === highlightedGroup["label"];

export async function getBinSizes(dashboardID) {
  const sizeQuery = bodybuilder()
    .size(0)
    .aggregation("stats", "x")
    .aggregation("stats", "y")
    .build();

  const sizeResults = await client.search({
    index: `dashboard_cells_${dashboardID.toLowerCase()}`,
    body: sizeQuery,
  });

  const { agg_stats_x, agg_stats_y } = sizeResults["aggregations"];

  const xBinSize = (agg_stats_x["max"] - agg_stats_x["min"]) / 100;
  const yBinSize = (agg_stats_y["max"] - agg_stats_y["min"]) / 100;

  return { xBinSize, yBinSize };
}

export const getDataMap = (results, xBinSize, yBinSize, getValue) => {
  const processYBuckets = (yBuckets) =>
    yBuckets.reduce(
      (yMap, bucket) => ({
        ...yMap,
        [Math.round(bucket["key"] / yBinSize)]: getValue(bucket),
      }),
      {}
    );

  return results["aggregations"]["agg_histogram_x"]["buckets"].reduce(
    (dataMap, xBucket) => ({
      ...dataMap,
      [Math.round(xBucket["key"] / xBinSize)]: processYBuckets(
        xBucket["agg_histogram_y"]["buckets"]
      ),
    }),
    {}
  );
};

const addFilter = (query, filter) => {
  if (filter["isNum"]) {
    // Is gene

    return query.query("nested", "path", "genes", (q) =>
      q
        .query("match", "genes.gene", filter["label"])
        .query("range", "genes.log_count", {
          gte: filter["value"][0],
          lt: filter["value"][1],
        })
    );
  } else {
    return query.filter("terms", filter["label"], filter["value"]);
  }
};
