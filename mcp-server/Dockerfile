# MongoDB MCP Server with Accept header proxy for Confluent Flink compatibility.
# The MCP SDK requires Accept: application/json, text/event-stream but Flink
# doesn't send it. A lightweight Node.js proxy on port 8080 adds the header
# before forwarding to mongodb-mcp-server on port 8000.
#
# base image pinned by digest (not just :24-alpine tag) so
# `docker buildx build --pull --no-cache` produces a reproducible image
# across deploys. Refresh with:
#   docker pull node:24-alpine
#   docker inspect --format='{{index .RepoDigests 0}}' node:24-alpine
FROM node:24-alpine@sha256:2bdb65ed1dab192432bc31c95f94155ca5ad7fc1392fb7eb7526ab682fa5bf14

RUN addgroup -S mcp && adduser -S mcp -G mcp
# version-pin the npm package so a malicious package update
# can't slip in between deploys. Refresh with `npm view mongodb-mcp-server version`.
# `--ignore-scripts` blocks postinstall hooks that would run as root.
RUN npm install -g --ignore-scripts mongodb-mcp-server@1.11.0
WORKDIR /home/mcp

COPY proxy.mjs /home/mcp/proxy.mjs
COPY start.sh /home/mcp/start.sh
RUN chmod +x /home/mcp/start.sh

ENV MDB_MCP_LOGGERS=stderr,mcp
ENV MDB_MCP_DISABLED_TOOLS=atlas-local,atlas-local-connect-deployment,atlas-local-create-deployment,atlas-local-delete-deployment,atlas-local-list-deployments,drop-database,drop-collection,drop-index,delete-many,rename-collection,export,create-collection,create-index,atlas-create-access-list,atlas-create-db-user,atlas-create-free-cluster,atlas-create-project
ENV MDB_MCP_EXTERNALLY_MANAGED_SESSIONS=true
ENV MDB_MCP_TRANSPORT=http
ENV MDB_MCP_HTTP_HOST=0.0.0.0
ENV MDB_MCP_HTTP_PORT=8000
ENV DOCKER_CONTAINER=1

USER mcp
EXPOSE 8080

ENTRYPOINT ["/home/mcp/start.sh"]
