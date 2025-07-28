#
# Builder
#
FROM node:22-alpine3.21 as builder

WORKDIR /opt/azurite

RUN apk add --no-cache curl unzip && \
    AZURITE_COMMIT=c3b8e4f012fc77002f723ca40390752b1fb48ccb && \
    curl -fsSL "https://github.com/nvartolomei/Azurite/archive/${AZURITE_COMMIT}.zip" -o azurite.zip && \
    unzip azurite.zip && \
    rm azurite.zip && \
    mv "Azurite-${AZURITE_COMMIT}"/* . && \
    rm -rf "Azurite-${AZURITE_COMMIT}" && \
    apk del curl unzip

RUN  npm ci --unsafe-perm
RUN npm run build && \
  npm install -g --unsafe-perm  --loglevel verbose


# Production image
#
FROM node:22-alpine3.21

ENV NODE_ENV=production

WORKDIR /opt/azurite

# Default Workspace Volume
VOLUME [ "/data" ]

COPY --from=builder /opt/azurite/package*.json /opt/azurite/LICENSE /opt/azurite/NOTICE.txt ./

COPY --from=builder /opt/azurite/dist/ dist/

RUN npm pkg set scripts.prepare="echo no-prepare"

RUN npm ci --unsafe-perm

RUN npm install -g --unsafe-perm --loglevel verbose

# Blob Storage Port
EXPOSE 10000
# Queue Storage Port
EXPOSE 10001
# Table Storage Port
EXPOSE 10002

CMD ["azurite", "-l", "/data", "--blobHost", "0.0.0.0","--queueHost", "0.0.0.0", "--tableHost", "0.0.0.0"]
