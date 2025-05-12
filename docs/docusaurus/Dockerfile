FROM node:21-alpine

WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json* ./
RUN npm install

# Copy everything else
COPY . .

# Set correct ownership for node user
RUN chown -R node:node /app

USER node

EXPOSE 3002

CMD ["npm", "start"]
