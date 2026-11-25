FROM node:20-alpine

# FFmpeg on Alpine already includes libopus
RUN apk add --no-cache ffmpeg

ENV NODE_ENV=production
WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force

COPY --chown=node:node server.js .

# Render injects PORT (default 10000); server.js reads process.env.PORT
USER node
EXPOSE 3000
CMD ["node", "server.js"]
