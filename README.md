# GraphQL Boilerplate

This project acts a boilerplate for the GraphQL layer of visualizations.

[React layer](https://github.com/shahcompbio/viz-react-boilerplate)
[GraphQL layer](https://github.com/shahcompbio/viz-graphql-boilerplate)

## Features

- Powered by [Apollo](https://www.apollographql.com/)
- Production build in [Docker](https://www.docker.com/)
- Snapshot testing with [Jest](https://jestjs.io/)

## Available Scripts

In the project directory, you can run:

### `yarn start`

Runs the app in the development mode.<br>
Open [http://localhost:4000/graphql](http://localhost:4000/graphql) to view the playground in the browser.

The page will reload if you make edits.<br>
You will also see any lint errors in the console.

### `yarn build`

Transpiles the app for production to the `lib` folder.

### `yarn test`

Runs all tests.

## Docker build

This project can be built for production and packaged with Docker. To do this:

```
docker build . -t graphql-boilerplate
docker run -d -p 4000:4000 --name graphql graphql-boilerplate
```
