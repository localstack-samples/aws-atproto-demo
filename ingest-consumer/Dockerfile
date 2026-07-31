# Runs the TypeScript source directly via tsx — no separate build step,
# matching the CDK Lambdas (also auto-bundled at deploy time).
FROM node:22-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY src ./src
CMD ["npx", "tsx", "src/index.ts"]
