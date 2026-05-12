FROM node:18-slim

# Install curl for healthchecks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install --only=production

COPY . .

# Run as non-root user
USER node

EXPOSE 3001

CMD ["node", "index.js"]
