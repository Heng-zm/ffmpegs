FROM node:20-alpine

    # Install FFmpeg and Opus audio codecs
    RUN apk add --no-cache ffmpeg opus

    WORKDIR /app

    COPY package*.json ./
    RUN npm install --production

    COPY server.js .

    EXPOSE 3000
    CMD ["node", "server.js"]
