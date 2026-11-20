FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY shared ./shared
COPY orchestrator ./orchestrator
CMD ["node", "--import", "tsx", "orchestrator/src/server.ts"]
