FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY shared ./shared
COPY observer ./observer
CMD ["node", "--import", "tsx", "observer/src/server.ts"]
