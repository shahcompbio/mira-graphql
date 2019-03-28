FROM node:8 as builder

WORKDIR /usr/src/app

COPY package*.json ./
RUN yarn install

COPY . .
RUN yarn build



FROM node:8
WORKDIR /root
COPY --from=builder /usr/src/app/lib ./lib
COPY --from=builder /usr/src/app/node_modules ./node_modules

EXPOSE 4000
CMD node lib/index.js
